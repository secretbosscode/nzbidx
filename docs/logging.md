# Logging

The API and ingest worker emit structured JSON logs. Each entry contains a timestamp,
log level, message, and contextual fields such as the request identifier so that
clients can trace a request across services.

## Log levels

Logging verbosity is controlled by the `LOG_LEVEL` environment variable. The
value is case-insensitive and defaults to `INFO` when unset. Setting
`LOG_LEVEL=DEBUG` enables verbose output for troubleshooting.

```bash
LOG_LEVEL=DEBUG uvicorn nzbidx_api.main:app
```

## Ingest log throttling

The ingest worker aggregates per-group metrics and emits a single
`ingest_summary` entry each poll cycle to avoid flooding the logs. Per-group
`ingest_batch` messages are also logged with runtime metrics.

## Library loggers

Third‑party libraries propagate to the root logger and therefore respect the
configured `LOG_LEVEL`. Noisy libraries can be tuned individually; for example,
`logging.getLogger("uvicorn.access").setLevel("WARNING")` silences access logs
while retaining other messages.

## Health check noise

Health check requests (`/health` and `/api/health`) can generate a large number
of access log entries. To reduce this noise, either raise the global log level
(e.g. `LOG_LEVEL=WARNING`) or disable Uvicorn's access log:

```bash
uvicorn nzbidx_api.main:app --no-access-log
```
