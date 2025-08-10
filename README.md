# nzbidx

NZBIdx is a lightweight, Newznab-compatible indexer built around two
Python services:

* **API** – a FastAPI/Starlette application exposing search and NZB
  retrieval endpoints backed by OpenSearch and Redis.
* **Ingest** – a small worker that parses NNTP headers, normalises
  subjects and indexes metadata into OpenSearch.

Only release metadata is stored; binaries are discarded during ingest.

## Quickstart

    docker compose up -d
    curl localhost:8080/health

The OpenAPI schema is available at `http://localhost:8080/openapi.json`.

> **Note**: OpenSearch requires `vm.max_map_count` to be at least 262144:

    sudo sysctl -w vm.max_map_count=262144

## Seed OpenSearch

    docker compose exec api python scripts/seed_os.py

OpenSearch uses an index template and ILM policy. Monthly indices are created
under the `nzbidx-releases` alias and rollover automatically. Indices move to
the warm phase after `ILM_WARM_DAYS` (default `14`) and are deleted after
`ILM_DELETE_DAYS` (default `180`).

## Ingest

The ingest worker polls configured NNTP groups and stores release metadata. Set the required NNTP environment variables and run:

    export NNTP_HOST=news.example.net
    export NNTP_GROUPS=alt.binaries.example
    docker compose exec ingest python -m nzbidx_ingest

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
`RETRY_JITTER_MS`.

### ILM & rollover

An ILM policy and index template manage monthly OpenSearch indices. The
template sets the `nzbidx-releases` write alias and rolls over after 30
days. Indices move to the warm phase after `ILM_WARM_DAYS` and are deleted
after `ILM_DELETE_DAYS`.

## Newznab Categories and Adult Content

The API exposes a handful of default category IDs:

    Movies: 2000
    TV: 5000
    Audio/Music: 3000
    Books/eBooks: 7000
    XXX/Adult: 6000

Set the following environment variables to override these defaults:

    MOVIES_CAT_ID
    TV_CAT_ID
    AUDIO_CAT_ID
    BOOKS_CAT_ID
    ADULT_CAT_ID

Example:

    export MOVIES_CAT_ID=1234

### SafeSearch and XXX Control

* `SAFESEARCH` (default `on`): when enabled, adult categories are removed from
  all responses and from the `caps` category listing. Set to `off` to allow
  adult categories to be returned.
* `ALLOW_XXX` (default `false`): when `false`, the XXX category (id `6000`) is
  entirely disabled. Requests that explicitly target this category receive an
  empty RSS feed with a comment noting that adult content is disabled. Enable
  this flag **and** set `SAFESEARCH=off` to expose XXX results.

## Production Deployment

Use the production override file to run the stack with persistent data stores and healthcheck ordering:

    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

## Configuration

- `ALLOW_XXX` controls whether adult content is searchable. It is disabled by
  default and must be explicitly set to `true` to opt in.
- Only metadata is indexed; binaries are stripped during ingest. An admin
  takedown endpoint is planned for future releases.

## Development

Run linters and tests for each service:

```
cd services/api
pip install -e .
ruff check src tests
pytest

    cd ../ingest
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

Manifests under ``k8s/`` provide minimal ``Deployment`` and ``Service`` examples for API and ingest with readiness and liveness probes.
