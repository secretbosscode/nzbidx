# nzbidx

If you find this project useful, donations are welcome at `BC1QTL4RMQJXTJ2K05UMGVWXL46SGR4DJG2GCMRR38`.

NZBIdx is a lightweight, Newznab-compatible indexer implemented as a single
Python application. It exposes FastAPI endpoints backed by OpenSearch and
Redis and runs a background worker that parses NNTP headers, normalises
subjects and indexes metadata into OpenSearch.

Only release metadata is stored; binaries are discarded during ingest.

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

## Performance Tuning

The default compose files apply a few settings aimed at keeping searches fast:

* **OpenSearch** runs with a fixed 512 MB heap via `OPENSEARCH_JAVA_OPTS` and uses
  a slower `30s` refresh interval with replicas disabled for better write throughput.
* **Redis** limits memory to 256 MB and evicts keys with a `volatile-lru` policy to
  keep frequently accessed results in RAM.

## Quickstart

    docker compose up -d
    curl localhost:8080/api/health
    curl localhost:8080/api/status

The OpenAPI schema is available at `http://localhost:8080/openapi.json`.

> **Note**: OpenSearch requires `vm.max_map_count` to be at least 262144:

    sudo sysctl -w vm.max_map_count=262144

### Use existing services

Already running Postgres, Redis, or an OpenSearch/ElasticSearch instance?
Create an override file and point `nzbidx` at your own services:

```yaml
services:
  nzbidx:
    environment:
      DATABASE_URL: postgresql+asyncpg://nzbidx:nzbidx@host.docker.internal:5432/nzbidx
      OPENSEARCH_URL: http://host.docker.internal:30003
      REDIS_URL: redis://host.docker.internal:6379/0
    depends_on: []
```

Start only the `nzbidx` service:

```bash
docker compose -f docker-compose.local.yml -f docker-compose.override.yml up -d nzbidx
```

Adjust hosts, ports, and credentials to match your environment.

If your OpenSearch or ElasticSearch instance requires authentication, embed
credentials in the URL:

```yaml
OPENSEARCH_URL: http://user:password@host.docker.internal:30003
```

## Environment Variables

Environment variables are defined directly in the compose files. Adjust values
there or set them via the environment before running the stack. Only a small set
of variables are required to run the stack:

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATABASE_URL` | Connection string for the application | `postgres://nzbidx:nzbidx@postgres:5432/nzbidx` |
| `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Postgres credentials for the database container | `nzbidx` |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Superuser applied to `schema.sql` during init | `nzbidx` |
| `POSTGRES_PORT` | Host port exposing Postgres | `15432` |
| `OPENSEARCH_URL` | OpenSearch endpoint; include `user:pass@` if authentication is required | `http://opensearch:9200` |
| `REDIS_URL` | Redis endpoint | `redis://redis:6379/0` |
| `REDIS_DISABLE_PERSISTENCE` | Disable Redis `save` and `appendonly` on startup (avoids writes to `/data`) | `0` |
| `REDIS_DATA_DIR` | Directory Redis uses for persistence (bind-mounted) | `/data` |
| `API_KEYS` | Comma separated API keys | _(empty)_ |
| `SAFESEARCH` | `on` hides adult categories | `on` |
| `ALLOW_XXX` | `true` enables the XXX category | `false` |
| `RATE_LIMIT` | Requests per window | `60` |
| `RATE_WINDOW` | Rate limit window in seconds | `60` |
| `NNTP_HOST` | NNTP provider host | _(required for ingest worker)_ |
| `NNTP_PORT` | NNTP port | `119` |
| `NNTP_SSL` | `1` enables SSL, `0` forces plaintext; auto when unset (SSL if port 563) | _(auto)_ |
| `NNTP_USER` | NNTP username | _(required for ingest worker)_ |
| `NNTP_PASS` | NNTP password | _(required for ingest worker)_ |
| `NNTP_GROUPS` | Groups to ingest (comma separated) | _(auto-discovered if unset)_ |
| `NNTP_IGNORE_GROUPS` | Groups to prune and ignore | _(none)_ |

Redis persistence remains unchanged unless `REDIS_DISABLE_PERSISTENCE` is set
to a truthy value. When enabled, the app issues `CONFIG SET save ""` and
`appendonly no` to avoid permission errors, and the bundled `docker-compose`
files start Redis with the same flags so nothing is written to `/data`. Use
`REDIS_DATA_DIR` to bind mount a custom directory for persistence if the default
`/data` path is unsuitable.

Additional optional variables tune behaviour (e.g. `SEARCH_TTL_SECONDS`,
`CORS_ORIGINS`, tracing via `OTEL_EXPORTER_OTLP_ENDPOINT`, or custom category
IDs such as `MOVIES_CAT_ID`). Review the configuration modules in
`services/api` for the full list. Consider whether extra variables are
necessary before adding them—defaults cover most cases.

## Seed OpenSearch

    docker compose exec nzbidx python scripts/seed_os.py

OpenSearch uses an index template and ILM policy. Monthly indices are created
under the `nzbidx-releases` alias and rollover automatically. Indices move to
the warm phase after `ILM_WARM_DAYS` (default `14`) and are deleted after
`ILM_DELETE_DAYS` (default `180`).

## Ingest

The API container also runs an ingest worker which polls NNTP groups and stores
release metadata. Set the required NNTP environment variables and start the
stack. When `NNTP_GROUPS` is omitted all available `alt.binaries.*` groups are
discovered automatically. To invoke a one-off ingest loop manually:

    export NNTP_HOST=news.example.net
    export NNTP_USER=username
    export NNTP_PASS=secret
    export NNTP_GROUPS=alt.binaries.example  # optional; auto-discovered if unset
    docker compose exec api python -m nzbidx_ingest

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

### Circuit breaker behavior

OpenSearch and Redis calls use a small circuit breaker with jittered
retries. After repeated failures the breaker opens and search endpoints
return empty responses while NZB retrieval returns HTTP 503. The circuit
half-opens after `CB_RESET_SECONDS` allowing a probe to close it on
success. Retry behaviour is tunable via `RETRY_MAX`, `RETRY_BASE_MS` and
`RETRY_JITTER_MS`. Current breaker states are exposed via
`/api/status`.

### ILM & rollover

An ILM policy and index template manage monthly OpenSearch indices. The
template sets the `nzbidx-releases` write alias and rolls over after 30
days. Indices move to the warm phase after `ILM_WARM_DAYS` and are deleted
after `ILM_DELETE_DAYS`.

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

### SafeSearch and XXX Control

* `SAFESEARCH` (default `off`): when enabled (`on`), adult categories are
  removed from all responses and from the `caps` category listing.
* `ALLOW_XXX` (default `true`): when `false`, the XXX category (id `6000`) is
  entirely disabled. Requests that explicitly target this category receive an
  empty RSS feed with a comment noting that adult content is disabled. Leave
  this flag enabled and ensure `SAFESEARCH=off` to expose XXX results.

## Production Deployment

Use the production override file to run the stack with persistent data stores and healthcheck ordering:

    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

## Configuration

- `ALLOW_XXX` controls whether adult content is searchable. It is enabled by
  default and can be set to `false` to disable it.
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
only needs ownership of the database—no additional privileges are required.

The application uses the `psycopg` driver for PostgreSQL connections. The Docker
images install it from `pyproject.toml`, but local environments may need to run
`pip install psycopg[binary]>=3.1`. Without the driver the ingest worker will
log `psycopg_unavailable` and fall back to SQLite.

## Backups

OpenSearch snapshots can be stored in S3 or GCS. Configure the repository via
environment variables and run the helper target (safe to run multiple times):

```bash
export OS_SNAP_REPO=nzbidx-snaps
export OS_S3_BUCKET=my-bucket
export OS_S3_REGION=us-east-1
# or GCS
# export OS_GCS_BUCKET=my-bucket
make snapshot-repo
```

### Schedule

Automated snapshots can be enabled by setting `SNAPSHOT_SCHEDULE=true` and
running `scripts/snapshot.sh` from `cron` or by enabling the scheduled workflow
in `.github/workflows/snapshot.yml`.

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

See [RUNBOOK.md](RUNBOOK.md) for common on-call checks.

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

## Kubernetes (examples only)

Manifests under ``k8s/`` provide a minimal ``Deployment`` and ``Service`` for the
API (which also runs the ingest worker) with readiness and liveness probes. See
[`docs/k8s.md`](docs/k8s.md) for usage notes and persistence considerations.


