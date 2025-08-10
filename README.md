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

> **Note**: OpenSearch requires `vm.max_map_count` to be at least 262144:

    sudo sysctl -w vm.max_map_count=262144

## Seed OpenSearch

    docker compose exec api python scripts/seed_os.py

## Ingest

The ingest worker polls configured NNTP groups and stores release metadata. Set the required NNTP environment variables and run:

    export NNTP_HOST=news.example.net
    export NNTP_GROUPS=alt.binaries.example
    docker compose exec ingest python -m nzbidx_ingest

## API Keys

Protect the `/api` endpoints by supplying one or more keys:

    export API_KEYS=dev
    curl -H 'X-Api-Key: dev' 'http://localhost:8080/api?t=caps'

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
