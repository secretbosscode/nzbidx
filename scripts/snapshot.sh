#!/usr/bin/env bash
set -euo pipefail

if [ "${SNAPSHOT_SCHEDULE:-false}" != "true" ]; then
  echo "snapshot schedule disabled" >&2
  exit 0
fi

OS_URL="${OPENSEARCH_URL:-http://localhost:9200}"
REPO="${OS_SNAP_REPO:-nzbidx-snaps}"
NAME="${1:-snap-$(date +%s)}"

curl -fsS -XPUT "$OS_URL/_snapshot/$REPO/$NAME?wait_for_completion=true" \
  -H 'Content-Type: application/json' -d '{}'

KEEP=${OS_SNAP_KEEP:-0}
if [ "$KEEP" -gt 0 ]; then
  mapfile -t snaps < <(curl -fsS "$OS_URL/_cat/snapshots/$REPO?h=id&s=start_epoch:desc")
  if [ "${#snaps[@]}" -gt "$KEEP" ]; then
    for snap in "${snaps[@]:$KEEP}"; do
      curl -fsS -XDELETE "$OS_URL/_snapshot/$REPO/$snap" >/dev/null || true
    done
  fi
fi
