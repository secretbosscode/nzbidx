#!/usr/bin/env bash
set -euo pipefail
URL=${1:-http://localhost:8080}
for i in {1..30}; do
  if curl -sf "$URL/health" > /dev/null; then
    break
  fi
  sleep 1
done
curl -sf "$URL/api?t=caps" > /dev/null
curl -sf "$URL/api?t=search&q=test" > /dev/null
echo "smoke ok"
