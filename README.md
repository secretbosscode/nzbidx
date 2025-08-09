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

## Production Deployment

Use the production override file to run the stack with persistent data stores and healthcheck ordering:

    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```
