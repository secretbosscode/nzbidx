# NZBIdx Runbook

Quick reference for common operational issues.

## Breaker stuck open
- **Symptoms:** search endpoints return empty arrays or `503` for NZB retrieval.
- **Checks:** `curl -fsS localhost:8080/health | jq .breaker` shows `open`.
- **Actions:** verify OpenSearch and Redis are reachable. Reset with
  `docker compose restart api` once dependencies are healthy.
- **Threshold hint:** alert if the breaker stays open >5m.

## 5xx spike
- **Symptoms:** sudden increase in `5xx` responses or alerts.
- **Checks:**
  - Logs: `docker compose logs api`
  - Dependencies: `curl -fsS localhost:8080/health`,
    `docker compose exec opensearch curl -fsS localhost:9200/_cluster/health`,
    `docker compose exec redis redis-cli PING`
  - Recent deploys: `git log -1 --oneline`
- **Actions:** inspect upstream failures, roll back or redeploy.
- **Threshold hint:** alert if 5xx rate >1% for 5m.

## Ingest lag
- **Symptoms:** ingest cursor behind the NNTP high-water mark.
- **Checks:**
  - Cursor: `sqlite3 services/ingest/cursors.sqlite 'select * from cursor'`
  - High-water mark: `docker compose exec ingest nntpstat <group>`
- **Actions:** ensure NNTP connectivity, lower `INGEST_BATCH` or raise
  `INGEST_POLL_SECONDS` to increase backpressure, restart ingest, or scale
  workers.
- **Threshold hint:** alert if lag >10k articles or >30m.

## Snapshot failures
- **Symptoms:** backup jobs error or snapshots missing.
- **Checks:** `docker compose logs api | grep snapshot`,
  `curl -fsS localhost:9200/_snapshot/_all`, `make snapshot-repo`.
- **Actions:** ensure repository registered and credentials valid; rerun
  `make snapshot-repo` and inspect OpenSearch logs.
- **Threshold hint:** alert if no successful snapshot in 24h.

## Useful commands
- Smoke test: `scripts/smoke.sh`
- Health check: `curl -fsS localhost:8080/health`
- Logs: `docker compose logs -f api ingest`
- OpenSearch slow logs:
  `docker compose exec opensearch tail -f /usr/share/opensearch/logs/opensearch_index_search_slowlog.log`
