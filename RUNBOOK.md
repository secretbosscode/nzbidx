# NZBIdx Runbook

Quick reference for common operational issues.

## Breaker stuck open
- **Symptoms:** search endpoints return empty arrays or `503` for NZB retrieval.
- **Checks:** inspect API logs for `breaker_open`; `curl localhost:8080/health`.
- **Actions:** verify OpenSearch and Redis are reachable. Restart the API if the
  dependencies are healthy but the breaker remains open for >5 minutes.

## 5xx spike
- **Symptoms:** sudden increase in `5xx` responses or alerts.
- **Checks:** `docker compose logs api`, `/health`, and tracing if enabled.
- **Actions:** look for upstream errors, redeploy if necessary.

## Ingest lag
- **Symptoms:** ingest cursor behind the NNTP high-water mark.
- **Checks:** `docker compose logs ingest`, monitor backlog depth.
- **Actions:** ensure NNTP connectivity, restart ingest, or scale workers.

## Snapshot failures
- **Symptoms:** backup jobs error or snapshots missing.
- **Checks:** `docker compose logs api | grep snapshot`,
  `curl localhost:9200/_snapshot/_all`.
- **Actions:** verify repository credentials and rerun `make snapshot-repo`.

## Useful commands
- Smoke test: `scripts/smoke.sh`
- Health check: `curl localhost:8080/health`
- Logs: `docker compose logs -f api ingest`
- OpenSearch slow logs:
  `docker compose exec opensearch tail -f /usr/share/opensearch/logs/opensearch_index_search_slowlog.log`
