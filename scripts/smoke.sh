#!/usr/bin/env bash
set -euo pipefail
URL=${1:-http://localhost:8080}
for i in {1..60}; do
  if curl -fs "$URL/api/health" >/dev/null; then
    break
  fi
  sleep 2
done
curl -fs "$URL/api/health" >/dev/null || {
  echo "health check failed" >&2
  exit 1
}
RID=${SMOKE_RID:-smoke-$(date +%s)}
check_rid() {
  local path=$1
  local hdrs
  hdrs=$(curl -fsD - -o /dev/null -H "X-Request-ID: $RID" "$URL$path")
  echo "$hdrs" | grep -qi "X-Request-ID: $RID" || {
    echo "request id mismatch on $path" >&2
    exit 1
  }
}
check_rid "/api/health"
check_rid "/api/status"
check_rid "/api?t=caps"
curl -fs "$URL/api?t=search&q=test" >/dev/null
echo "smoke ok"
