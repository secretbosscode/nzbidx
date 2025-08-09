```markdown
# nzbidx

Placeholder for project description.

## Quickstart

    docker compose up -d
    curl localhost:8080/health

> **Note**: OpenSearch requires `vm.max_map_count` to be at least 262144:

    sudo sysctl -w vm.max_map_count=262144

## Seed OpenSearch

    docker compose exec api python scripts/seed_os.py

## SafeSearch and Adult Content

The API exposes two environment flags to control adult content:

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
```
