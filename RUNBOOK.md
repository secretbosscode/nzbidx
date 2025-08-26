# NZBidx Runbook

Quick reference for common operational issues.

## SLOs
- **API availability:** 99.9% success rate over 30d
- **Breaker stability:** <1 forced reset per hour
- **Ingest freshness:** lag <10k articles or <30m
- **Snapshots:** at least one successful snapshot every 24h

## PostgreSQL requirements
Connections to PostgreSQL require the `psycopg` driver. The Docker images
install `psycopg[binary] >= 3.1` from the service `pyproject.toml` files. The
`pg_trgm` and `vector` extensions must be installed by a superuser; the init
script at `db/init/schema.sql` handles this during database provisioning.

The schema creates the `search_vector` column and `release_search_idx` index.
Verify the index exists:

```bash
psql "$DATABASE_URL" -c "SELECT to_regclass('release_search_idx');"
```

`to_regclass` returns `release_search_idx` when the index exists. See
[docs/db.md#full-text-search](docs/db.md#full-text-search).

## Engine lifecycle
`init_engine()` binds the async database engine to the currently running event loop.
Call `dispose_engine()` on that same loop before it closes to avoid cross-loop
`RuntimeError` from asyncpg.

```python
import asyncio
from nzbidx_api.db import init_engine, dispose_engine

async def main():
    await init_engine()
    try:
        ...  # application logic
    finally:
        await dispose_engine()

asyncio.run(main())
```

## Release table partitioning
The `release` table must be partitioned by `category_id` so partitions can be
maintained independently. The application migrates and creates this table automatically on startup when provided a `DATABASE_URL` with superuser privileges. For reference, create the table and partitions:

```sql
CREATE TABLE IF NOT EXISTS release (
    id BIGSERIAL PRIMARY KEY,
    norm_title TEXT,
    category TEXT,
    category_id INT,
    language TEXT NOT NULL DEFAULT 'und',
    tags TEXT NOT NULL DEFAULT '',
    source_group TEXT,
    size_bytes BIGINT,
    segments JSONB,
    has_parts BOOLEAN NOT NULL DEFAULT FALSE,
    part_count INT NOT NULL DEFAULT 0
) PARTITION BY RANGE (category_id);

CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release
    FOR VALUES FROM (2000) TO (3000);
CREATE TABLE IF NOT EXISTS release_music PARTITION OF release
    FOR VALUES FROM (3000) TO (4000);
CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release
    FOR VALUES FROM (5000) TO (6000);
CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release
    FOR VALUES FROM (6000) TO (7000);
CREATE TABLE IF NOT EXISTS release_books PARTITION OF release
    FOR VALUES FROM (7000) TO (8000);
CREATE TABLE IF NOT EXISTS release_other PARTITION OF release DEFAULT;
```

Startup verifies that the `release_adult` table is partitioned. If it remains
unpartitioned, the service raises a `RuntimeError` and refuses to continue
until the migration succeeds.

## Graceful shutdown
- **Steps:**
  1. Wait for outstanding tasks to complete.
  2. Call `dispose_engine()` to close database connections.
  3. Stop the event loop.
- **Why:** skipping these steps may trigger `RuntimeError: got Future attached to a different loop` in asyncpg.

## Breaker stuck open
- **Symptoms:** search endpoints return empty arrays or `503` for NZB retrieval.
- **Checks:** `curl -fsS localhost:8080/health | jq .breaker` shows `open`.
- **Actions:** restart the service once dependencies are healthy using
  `docker compose restart nzbidx`.
- **Threshold hint:** alert if the breaker stays open >5m.
- **PromQL:** `nzbidx_breaker_state == 1`
- **Alert rule:**
  ```yaml
  - alert: NZBidxBreakerOpen
    expr: nzbidx_breaker_state == 1
    for: 5m
    labels: {severity: page}
    annotations: {summary: "Circuit breaker open"}
  ```
- **Reset:** `docker compose restart nzbidx`

## 5xx spike
- **Symptoms:** sudden increase in `5xx` responses or alerts.
- **Checks:**
  - Logs: `docker compose logs nzbidx`
  - Dependencies: `curl -fsS localhost:8080/health`
  - Recent deploys: `git log -1 --oneline`
- **Actions:** inspect upstream failures, roll back or redeploy.
- **Threshold hint:** alert if 5xx rate >1% for 5m.
- **PromQL:** `sum(rate(http_server_errors_total[5m])) / sum(rate(http_requests_total[5m])) > 0.01`
- **Alert rule:**
  ```yaml
  - alert: NZBidx5xxRateHigh
    expr: sum(rate(http_server_errors_total[5m])) / sum(rate(http_requests_total[5m])) > 0.01
    for: 5m
    labels: {severity: page}
    annotations: {summary: "5xx rate above 1%"}
  ```

## Ingest lag
- **Symptoms:** ingest cursor behind the NNTP high-water mark.
- **Checks:**
  - Cursor: `sqlite3 services/api/cursors.sqlite 'select * from cursor'`
  - High-water mark: `docker compose exec nzbidx nntpstat <group>`
- **Actions:** ensure NNTP connectivity, lower `INGEST_BATCH_MAX` or raise
  `INGEST_POLL_MIN_SECONDS`/`INGEST_POLL_MAX_SECONDS` to increase
  backpressure, restart ingest, or scale workers. Benchmarks set the
  defaults at `INGEST_BATCH_MAX=1000`, `INGEST_BATCH_MIN=100`, and polling
  between 5–60 seconds (`INGEST_POLL_MIN_SECONDS=5`,
  `INGEST_POLL_MAX_SECONDS=60`) to balance throughput and load.
- **Threshold hint:** alert if lag >10k articles or >30m.
- **PromQL:** `nzbidx_ingest_lag_articles > 10000 or nzbidx_ingest_lag_seconds > 1800`
- **Alert rule:**
  ```yaml
  - alert: NZBidxIngestLag
    expr: nzbidx_ingest_lag_articles > 10000 or nzbidx_ingest_lag_seconds > 1800
    for: 10m
    labels: {severity: ticket}
    annotations: {summary: "Ingest lagging"}
  ```


## Missing database extensions
- **Symptoms:** warnings about `extension_unavailable` during API startup or errors
  referencing `pg_trgm` or `vector`.
- **Checks:** `docker compose logs postgres`, `psql -c '\dx'` to list installed
  extensions.
- **Actions:** ensure `db/init/schema.sql` ran under a superuser (via
  `POSTGRES_USER`).  If using an external database, run the script manually or
  ask an administrator to install `pg_trgm` and `vector`.

## Missing database driver
- **Symptoms:** startup logs show `ModuleNotFoundError: No module named 'psycopg'`
  or `psycopg_unavailable` and the application exits.
- **Checks:** `pip show psycopg` or rebuild the service image.
- **Actions:** install the driver with `pip install psycopg[binary]>=3.1` or
  rebuild the Docker image so it installs dependencies from `pyproject.toml`.

## Release IDs
- **Definition:** `normalize_subject(title):<posted-date>` where
  `<posted-date>` is `YYYY-MM-DD`.
- **Example:**
  ```python
  from nzbidx_ingest.parsers import normalize_subject

  subject = "Example.part01.rar"
  release_id = f"{normalize_subject(subject)}:2025-08-17"
  ```
- **Usage:** pass the ID to `t=getnzb` requests.

## Backfilling segments
- **Symptoms:** NZB requests return `404` with `nzb fetch failed: release has no segments` or `nzb fetch failed: release not found`.
- **Checks:** missing data in the `segments` column or the release absent; ensure the release ID is normalized.
- **Actions:** repopulate segments with `docker compose exec nzbidx python scripts/backfill_release_parts.py`. The helper prunes invalid releases, so verify the ID before retrying.


## Full reindex
- **Usage:** `python scripts/reindex_all.py`
- **Runtime:** roughly 1 minute per 100k releases

## Useful commands
- Smoke test: `scripts/smoke.sh`
- Health check: `curl -fsS localhost:8080/health`
- Logs: `docker compose logs -f nzbidx`
