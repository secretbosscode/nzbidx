#!/usr/bin/env bash
set -e

python -m nzbidx_ingest &
INGEST_PID=$!

uvicorn nzbidx_api.main:app --host 0.0.0.0 --port 8080 &
API_PID=$!

# Ensure both processes are terminated on exit
trap 'kill $INGEST_PID $API_PID 2>/dev/null || true' INT TERM EXIT

wait -n $INGEST_PID $API_PID
EXIT_CODE=$?
kill $INGEST_PID $API_PID 2>/dev/null || true
wait
exit $EXIT_CODE
