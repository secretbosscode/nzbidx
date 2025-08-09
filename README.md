# nzbidx

Placeholder for project description.

## Quickstart

```bash
docker compose up -d
curl localhost:8080/health
```

> **Note**: OpenSearch requires `vm.max_map_count` to be at least 262144:

```bash
sudo sysctl -w vm.max_map_count=262144
```

## Seed OpenSearch

```bash
docker compose exec api python scripts/seed_os.py
```
