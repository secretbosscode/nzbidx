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

## Breaker stuck open
- **Symptoms:** search endpoints return empty arrays or `503` for NZB retrieval.
- **Checks:** `curl -fsS localhost:8080/health | jq .breaker` shows `open`.
- **Actions:** verify OpenSearch is reachable. Reset with
  `docker compose restart nzbidx` once dependencies are healthy.
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
  - Dependencies: `curl -fsS localhost:8080/health`,
    `docker compose exec opensearch curl -fsS localhost:9200/_cluster/health`,
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

## Snapshot failures
- **Symptoms:** backup jobs error or snapshots missing.
- **Checks:** `docker compose logs nzbidx | grep snapshot`,
  `curl -fsS localhost:9200/_snapshot/_all`, `make snapshot-repo`.
- **Actions:** ensure repository registered and credentials valid; rerun
  `make snapshot-repo` and inspect OpenSearch logs.
- **Threshold hint:** alert if no successful snapshot in 24h.
- **PromQL:** `time() - nzbidx_snapshot_last_success > 86400`
- **Alert rule:**
  ```yaml
  - alert: NZBidxSnapshotStale
    expr: time() - nzbidx_snapshot_last_success > 86400
    for: 10m
    labels: {severity: ticket}
    annotations: {summary: "No snapshot in 24h"}
  ```
- **Manual run:** `bash scripts/snapshot.sh`
- **Retention:** set `OS_SNAP_KEEP` to limit repository size; `snapshot.sh` prunes older snapshots.

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
- **Definition:** `normalize_subject(title).lower():<posted-date>` where
  `<posted-date>` is `YYYY-MM-DD`.
- **Example:**
  ```python
  from nzbidx_ingest.parsers import normalize_subject

  subject = "Example.part01.rar"
  release_id = f"{normalize_subject(subject).lower()}:2025-08-17"
  ```
- **Usage:** pass the ID to `t=getnzb` requests.

## Backfilling segments
- **Symptoms:** NZB requests return `404` with `nzb fetch failed: release has no segments` or `nzb fetch failed: release not found`.
- **Checks:** missing data in the `segments` column or the release absent; ensure the release ID is normalized.
- **Actions:** repopulate segments with `docker compose exec nzbidx python scripts/backfill_release_parts.py`. The helper prunes invalid releases, so verify the ID before retrying.

## OpenSearch orphan cleanup
- **Symptoms:** documents exist in `nzbidx-releases` without matching rows in the `release` table.
- **Checks:** `python scripts/check_os_orphans.py` logs the number of orphans.
- **Actions:** remove them with `python scripts/check_os_orphans.py --prune`.
- **Schedule:** run nightly via cron, e.g., `0 4 * * * python scripts/check_os_orphans.py --prune`.

## Full reindex
- **Usage:** `python scripts/reindex_all.py`
- **Runtime:** roughly 1 minute per 100k releases

## Useful commands
- Smoke test: `scripts/smoke.sh`
- Health check: `curl -fsS localhost:8080/health`
- Logs: `docker compose logs -f nzbidx`
- OpenSearch slow logs:
  `docker compose exec opensearch tail -f /usr/share/opensearch/logs/opensearch_index_search_slowlog.log`
