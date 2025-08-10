# NZBIdx Runbook

Quick reference for common operational issues.

## Breaker stuck open
- **Symptoms:** search endpoints return empty arrays or `503` for NZB retrieval.
- **Checks:**
  - `curl -sS localhost:8080/health | jq .breaker_open`
  - `docker compose logs api | grep breaker_open`
- **Actions:** verify OpenSearch and Redis are reachable. Restart the API if
  dependencies are healthy but the breaker remains open.
- **Threshold hint:** alert if `breaker_open` persists for more than 5 minutes.

## 5xx spike
- **Symptoms:** sudden increase in `5xx` responses or alerts.
- **Checks:**
  - `docker compose logs --since 10m api`
  - `curl -sS localhost:8080/health`
  - check recent deploys: `git log -1`
  - confirm dependencies: `docker compose ps`
- **Actions:** look for upstream errors, roll back recent deploys or restart
  services.
- **Threshold hint:** alert if `5xx` rate exceeds 1% over 5 minutes.

## Ingest lag
- **Symptoms:** ingest cursor behind the NNTP high-water mark.
- **Checks:**
  - cursor position:
    `docker compose exec ingest sqlite3 cursors.sqlite 'SELECT group, last_article FROM cursor;'`
  - high-water mark:
    `docker compose exec ingest bash -c 'printf "GROUP <group>\r\n" | nc $NNTP_HOST $NNTP_PORT'`
  - backlog depth: `docker compose logs --since 5m ingest`
- **Actions:** ensure NNTP connectivity. Adjust backpressure by tuning
  `INGEST_OS_LATENCY_MS` and restarting ingest or scaling workers.
- **Threshold hint:** alert if cursor lags more than 5k articles or 10 minutes.

## Snapshot failures
- **Symptoms:** backup jobs error or snapshots missing.
- **Checks:**
  - `docker compose logs api | grep snapshot`
  - `curl -sS localhost:9200/_snapshot/_all`
  - repository registration: `docker compose exec api python scripts/os_snapshot_repo.py --check`
- **Actions:** verify repository credentials and rerun `make snapshot-repo`.
- **Threshold hint:** alert after two consecutive snapshot failures.

## Useful commands
- Smoke test: `scripts/smoke.sh`
- Health check: `curl -sS localhost:8080/health`
- Logs: `docker compose logs -f api ingest`
- OpenSearch slow logs:
  `docker compose exec opensearch tail -f /usr/share/opensearch/logs/opensearch_index_search_slowlog.log`
