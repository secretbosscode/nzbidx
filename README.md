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

## Newznab Categories

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

## Production Deployment

Use the production override file to run the stack with persistent data stores and healthcheck ordering:

    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```
