# NZBidx

If you find this project useful, donations are welcome at `BC1QTL4RMQJXTJ2K05UMGVWXL46SGR4DJG2GCMRR38`.

NZBidx is a lightweight, Newznab-compatible indexer implemented as a single
Python application. It exposes Starlette endpoints backed by PostgreSQL and
runs a background worker that parses NNTP headers and stores release metadata
in the database.

Only release metadata is stored; binaries are discarded during ingest.

Python 3.13 removed the standard library `nntplib` module; NZBidx depends on
the maintained `standard-nntplib` package to provide NNTP client support.

## Database Performance

Postgres uses a set of indexes to keep queries fast as the dataset grows. The
schema creates the indexes during initialization; see `db/init/schema.sql` for
details.

Connections to PostgreSQL require the [`psycopg` driver](https://www.psycopg.org/).
The container images install `psycopg[binary] >= 3.1` from the application's
`pyproject.toml`. The `pg_trgm` extension must be installed by a
superuser—`docker-compose` mounts `db/init/schema.sql` so the database is
provisioned with the required extension during initialisation. Having permission
to create databases is not enough; roles without superuser rights cannot install
extensions.

Routine maintenance keeps PostgreSQL statistics and indexes fresh. The
`scripts/db_maintenance.py` helper schedules `VACUUM (ANALYZE)`, `ANALYZE`,
and `REINDEX` jobs via APScheduler.

## Database Initialization

Seed a fresh PostgreSQL instance before starting ingestion. Apply the schema
file to install required extensions, create the partitioned `release` table,
and add the full-text search column and index:

```bash
psql "$DATABASE_URL" -f db/init/schema.sql
psql "$DATABASE_URL" -c "SELECT to_regclass('release_search_idx');"
```

Run these commands once with a superuser account or via your migration tool.
The `to_regclass` query returns `release_search_idx` when the schema is applied.
See [docs/db.md#full-text-search](docs/db.md#full-text-search) for details.

The application migrates the `release` table automatically on startup when
provided a `DATABASE_URL` with superuser privileges, so no separate migration
script is required.

## Performance Tuning

The default compose files apply a few settings aimed at keeping searches fast:


## Quickstart

    docker compose up -d
    curl localhost:8080/api/health
    curl localhost:8080/api/status

The OpenAPI schema is available at `http://localhost:8080/openapi.json`.


Both the ingest worker and the API must use the **same persistent database**
specified via `DATABASE_URL`. Using different databases or an in-memory
SQLite database prevents NZB downloads from working.

## Environment Variables

Environment variables are defined directly in the compose files. Adjust values
there or set them via the environment before running the stack. Only a small set
of variables are required to run the stack:

The compose files and Docker image default to the standard library `json`
module (`NZBIDX_USE_STD_JSON=1`) for maximum compatibility because `orjson`
may not support the latest Python release. Install the optional `orjson` extra with
`pip install "nzbidx-api[orjson]"` and set `NZBIDX_USE_STD_JSON=0` to enable the
faster serializer once compatible.

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATABASE_URL` | Connection string for the application | `postgres://nzbidx:nzbidx@postgres:5432/nzbidx` |
| `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Postgres credentials for the database container | `nzbidx` |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Superuser applied to `schema.sql` during init | `nzbidx` |
| `POSTGRES_PORT` | Host port exposing Postgres | `15432` |
| `API_KEYS` | Comma separated API keys; accepted via `X-Api-Key` header, `apikey` query parameter, or HTTP Basic auth | _(empty)_ |
| `RATE_LIMIT` | Requests per window | `60` |
| `RATE_WINDOW` | Rate limit window in seconds | `60` |
| `NZBIDX_USE_STD_JSON` | `0` uses `orjson` if installed; `1` or unset uses the standard library `json` module | `1` |
| `NZB_TIMEOUT_SECONDS` | Maximum seconds to fetch an NZB before failing (≥ `NNTP_TOTAL_TIMEOUT`) | `NNTP_TOTAL_TIMEOUT` (`600`) |
| `NNTP_HOST` | NNTP provider host | _(required for ingest worker)_ |
| `NNTP_PORT` | NNTP port | `119` |
| `NNTP_SSL` | `1` enables SSL, `0` forces plaintext; auto when unset (SSL if port 563) | _(auto)_ |
| `NNTP_USER` | NNTP username | _(required for ingest worker)_ |
| `NNTP_PASS` | NNTP password | _(required for ingest worker)_ |
| `NNTP_GROUPS` | NNTP groups to scan (comma separated, wildcards allowed) | _(none)_ |
| `NNTP_GROUP_WILDCARD` | Pattern used when discovering groups | `alt.binaries.*` |
| `NNTP_GROUP_LIMIT` | Maximum groups to enumerate when using wildcards | _(unlimited)_ |
| `NNTP_IGNORE_GROUPS` | Groups to prune and ignore | _(none)_ |
| `NNTP_TIMEOUT` | Socket timeout for NNTP connections in seconds (increase for slow or flaky providers) | `30` |
| `NNTP_TOTAL_TIMEOUT` | Maximum total seconds for NNTP attempts across retries (API timeout should be ≥ this) | `600` |
| `DETECT_LANGUAGE` | `1` enables automatic language detection (`0` disables for faster ingest) | `1` |

> **Note**: To avoid premature API timeouts during NZB generation, ensure
> `NZB_TIMEOUT_SECONDS` is greater than or equal to `NNTP_TOTAL_TIMEOUT`.


Language detection improves the accuracy of release metadata but incurs a
performance cost. Results are cached using an LRU strategy, yet disabling
language detection (`DETECT_LANGUAGE=0`) may be desirable when ingest speed is
preferred over precise language tags.

Additional optional variables tune behaviour (e.g. `SEARCH_TTL_SECONDS`,
`CORS_ORIGINS`, tracing via `OTEL_EXPORTER_OTLP_ENDPOINT`, or custom category
IDs such as `MOVIES_CAT_ID`). Review the configuration modules in
`services/api` for the full list. Consider whether extra variables are
necessary before adding them—defaults cover most cases.


    docker compose exec nzbidx python scripts/backfill_release_parts.py

The script inserts segment metadata for each release and drops entries that no
longer resolve via NNTP.

If NZB retrieval reports `release has no segments`, run the helper to
repopulate the record. A `release not found` error means the release was
removed—either never ingested or pruned as invalid. The backfill script may
remove releases that no longer resolve, so verify the release ID is normalized
before retrying.

To repair any future releases that lose their segment metadata, schedule the
auto mode which scans for entries with `has_parts=true` but missing `segments`:

    docker compose exec nzbidx python scripts/backfill_release_parts.py --auto

The helper reuses the backfill logic to repopulate missing segments or mark
unrecoverable releases as unavailable.

An authenticated admin endpoint can also trigger the job asynchronously:

    curl -X POST -H 'X-Api-Key: dev' http://localhost:8080/api/admin/backfill

Repeat requests report the current progress while the backfill runs.

## Ingest

The API container also runs an ingest worker which polls NNTP groups and stores
release metadata. Set the required NNTP environment variables and start the
stack. Supply `NNTP_GROUPS` with a curated comma-separated list for better
performance. Wildcard patterns like `alt.binaries.*` (controlled by
`NNTP_GROUP_WILDCARD`) are expanded via `server.list`; set `NNTP_GROUP_LIMIT` to
cap enumeration. Use `NNTP_TIMEOUT` to
adjust the socket timeout for slow or flaky providers and `NNTP_TOTAL_TIMEOUT`
to cap overall retry time. To invoke a one-off ingest loop manually:

    export NNTP_HOST=news.example.net
    export NNTP_USER=username
    export NNTP_PASS=secret
    export NNTP_GROUPS=alt.binaries.example  # recommended to set explicitly
    docker compose exec api python -m nzbidx_ingest

### Ingest Checklist

- Set the required NNTP variables before starting the worker: `NNTP_HOST`,
  `NNTP_PORT`, `NNTP_USER`, `NNTP_PASS`, and `NNTP_GROUPS`.
- Backfill segment data for a specific release with its numeric ID:

      docker compose exec nzbidx python scripts/backfill_release_parts.py <release-id>

  Replace `<release-id>` with the target `release.id`.
- Verify the API and ingest worker share the same database connection:

      docker compose exec api printenv DATABASE_URL
      docker compose exec nzbidx printenv DATABASE_URL

  Both commands should return the same `DATABASE_URL` value.

## Auth & Rate Limit

Protect the `/api` endpoints by supplying one or more keys:

    export API_KEYS=dev
    curl -H 'X-Api-Key: dev' 'http://localhost:8080/api?t=caps'

Requests are limited per IP using `RATE_LIMIT` requests per `RATE_WINDOW`
seconds. Exceeding the limit returns HTTP 429.

## Pagination & Caching

RSS endpoints accept `limit` (default `50`, max `100`) and `offset` query
parameters. Requests exceeding the maximum return HTTP 400. Successful
responses include `Cache-Control: public, max-age=<SEARCH_TTL_SECONDS>` and an
`ETag` header; subsequent requests with `If-None-Match` receive HTTP 304.
Supplying `Cache-Control: no-cache` forces a fresh response.

## Security Headers

All responses include basic security headers (`X-Content-Type-Options:
nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`,
`X-Download-Options: noopen`, `Permissions-Policy: interest-cohort=()`).
Set `CORS_ORIGINS` to enable CORS for specific origins.

### Request ID & tracing

Each response carries an `X-Request-ID` header. Clients may supply their own
identifier (default `X-Request-ID`, configurable via `REQUEST_ID_HEADER`) to
aid correlation. Structured logs include this `request_id` field.

Tracing is automatically enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
Override the service name with `OTEL_SERVICE_NAME`.

`RETRY_JITTER_MS`. Current breaker states are exposed via
`/api/status`.


## Newznab Categories and Adult Content

The API exposes the full predefined set of category IDs by default.
Categories can be customized without redeploying by providing a JSON
configuration file via the ``CATEGORY_CONFIG`` environment variable. The file
should contain an array of objects with ``id`` and ``name`` keys, for example:

```json
[
  {"id": 2000, "name": "Movies"},
  {"id": 5000, "name": "TV"}
]
```

When ``CATEGORY_CONFIG`` is not set or cannot be read, the predefined categories
listed below are used. The individual ``*_CAT_ID`` environment variables remain
supported as fallbacks for overriding the built-in defaults.

### Predefined Category IDs

| ID   | Category Name       |
| ---- | ------------------- |
| 0000 | Reserved            |
| 1000 | Console             |
| 1010 | Console/NDS         |
| 1020 | Console/PSP         |
| 1030 | Console/Wii         |
| 1040 | Console/XBox        |
| 1050 | Console/XBox 360    |
| 1060 | Console/Wiiware     |
| 1070 | Console/XBox 360 DLC|
| 2000 | Movies              |
| 2010 | Movies/Foreign      |
| 2020 | Movies/Other        |
| 2030 | Movies/SD           |
| 2040 | Movies/HD           |
| 2050 | Movies/BluRay       |
| 2060 | Movies/3D           |
| 3000 | Audio               |
| 3010 | Audio/MP3           |
| 3020 | Audio/Video         |
| 3030 | Audio/Audiobook     |
| 3040 | Audio/Lossless      |
| 4000 | PC                  |
| 4010 | PC/0day             |
| 4020 | PC/ISO              |
| 4030 | PC/Mac              |
| 4040 | PC/Mobile-Other     |
| 4050 | PC/Games            |
| 4060 | PC/Mobile-iOS       |
| 4070 | PC/Mobile-Android   |
| 5000 | TV                  |
| 5020 | TV/Foreign          |
| 5030 | TV/SD               |
| 5040 | TV/HD               |
| 5050 | TV/Other            |
| 5060 | TV/Sport            |
| 6000 | XXX                 |
| 6010 | XXX/DVD             |
| 6020 | XXX/WMV             |
| 6030 | XXX/XviD            |
| 6040 | XXX/x264            |
| 7000 | Other               |
| 7010 | Misc                |
| 7020 | EBook               |
| 7030 | Comics              |

## Production Deployment

Use the production override file to run the stack with persistent data stores and healthcheck ordering:

    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

## Configuration

- Only metadata is indexed; binaries are stripped during ingest. An admin
  takedown endpoint (`POST /api/admin/takedown`) accepts a release ID and removes
  it from the search index. The endpoint requires a valid `X-Api-Key`.

## Development

Run linters and tests:

```
cd services/api
pip install -e .
ruff check src tests
pytest
```

## Database

Development and tests default to SQLite and create tables on startup. To use
PostgreSQL in production (e.g. docker-compose), set a `DATABASE_URL`:

```bash
DATABASE_URL=postgres://nzbidx:nzbidx@postgres:5432/nzbidx
```

Schema creation is idempotent; running with a new database is sufficient.
The required `vector` and `pg_trgm` extensions must be installed by a
superuser before the application starts. Having `CREATE DATABASE` privileges
alone won't work. The compose files mount `db/init/schema.sql`, which installs
the extensions and creates tables during startup as `POSTGRES_USER`. When using
an external Postgres instance or setting up the database manually, create the
role and database, install the extensions and apply the schema as described in
[docs/db.md](docs/db.md). Once the extensions are installed the `nzbidx` role
only needs ownership of the database—no additional privileges are required. The
schema setup routine drops any superuser rights from the role as a final
cleanup step, ensuring it runs as a regular user afterward.

The application uses the `psycopg` driver for PostgreSQL connections. The Docker
images install it from `pyproject.toml`, but local environments may need to run
`pip install psycopg[binary]>=3.1`. Without the driver the ingest worker will
log `psycopg_unavailable` and fall back to SQLite.

## Release IDs

Releases are addressed by a stable ID combining the normalized title and the
date the post was seen.  The ID format is
``normalize_subject(title):<posted-date>`` where `<posted-date>` is in
`YYYY-MM-DD` form.

Derive an ID from a raw subject:

```python
from nzbidx_ingest.parsers import normalize_subject

subject = "[01/15] - Some.Release.part01.rar"
title = normalize_subject(subject)
release_id = f"{title}:2025-08-17"
```

Use the resulting ID with the `t=getnzb` endpoint (URL-encoded if needed):

```bash
curl -H 'X-Api-Key: dev' \
  'http://localhost:8080/api?t=getnzb&id=some%20release:2025-08-17'
```

## Backfilling segments

NZB downloads rely on segment metadata stored in the `segments` column. If this
data is missing—for example after migrating an older database—NZB requests will
fail with `404` responses such as `nzb fetch failed: no segments for release`.
Populate missing segments with:

```
docker compose exec nzbidx python scripts/backfill_release_parts.py
```


Set `OS_SNAP_KEEP` to the number of snapshots to retain. Older snapshots will
be removed after each run to keep the repository size in check.

### Restore

1. Register the repository if needed.
2. List snapshots via `GET /_snapshot/<repo>/_all`.
3. Restore and remap the alias:

```bash
curl -XPOST \
  http://localhost:9200/_snapshot/<repo>/<snap>/_restore \
  -H 'Content-Type: application/json' -d '
{
  "indices": "nzbidx-releases-*",
  "aliases": {"nzbidx-releases": {"is_write_index": true}}
}
'
```

Close indices if required; restoring over an existing alias may need
`rename_pattern`/`rename_replacement`.

## Security

Run `pip-audit` and generate a CycloneDX SBOM to identify vulnerabilities.
Local audit and SBOM generation:

```bash
pip install pip-audit cyclonedx-py
pip install -e services/api
pip-audit
cyclonedx-py environment -o sbom.json
```

See [docs/sbom.md](docs/sbom.md) for details.

## Operations

See [RUNBOOK.md](RUNBOOK.md) for common on-call checks. The [engine lifecycle
section](RUNBOOK.md#engine-lifecycle) covers the loop-bound `init_engine()` and
the requirement to call `dispose_engine()` before the originating event loop
closes.

Container logs stream to stdout/stderr. Configure log rotation (daily or by size)
to avoid unbounded growth. Example Docker daemon snippet:

```json
{
  "log-driver": "json-file",
  "log-opts": {"max-size": "50m", "max-file": "7"}
}
```

## Smoke Test

Bring up the stack and run the smoke script which checks health, `caps` and a sample search:

```bash
docker compose up -d && scripts/smoke.sh
```

## Troubleshooting

- ``vm.max_map_count`` too low – run ``sudo sysctl -w vm.max_map_count=262144``.
- ``401`` – missing or invalid ``X-Api-Key`` header.
- ``429`` – per-key quota or rate limit exceeded.
- ``503`` – circuit breaker open or backend unavailable.
- Port 8080 in use – remap in ``docker-compose.yml`` via ``ports: ["18080:8080"]``.
- Dependencies unhealthy – ``docker compose ps`` and ``docker compose logs <svc>``.
- Snapshot repo not configured – ``make snapshot-repo`` no-ops until env vars set.
- Smoke test times out – increase timeout or inspect ``docker compose logs``.
- ``t=movie`` without a ``q`` parameter returns a placeholder "Indexer Test Item" when no real results exist.
- Inspect segment metadata – `python scripts/check_release_segments.py <id>` prints segment count and first/last message IDs.

## Kubernetes (examples only)

Manifests under ``k8s/`` provide a minimal ``Deployment`` and ``Service`` for the
API (which also runs the ingest worker) with readiness and liveness probes. See
[`docs/k8s.md`](docs/k8s.md) for usage notes and persistence considerations.


