# Logging

The API emits structured logs to aid diagnostics. Timeout errors during NZB
retrieval log a message similar to:

```
"nzb fetch timed out after 30s"
```

Each timeout log includes the `release_id` and `request_id` fields so that
clients can correlate the entry with a specific request.

## Ingest worker

The ingest process emits an `ingest_batch` log for each group processed. These
entries are written at the `DEBUG` level by default. Set the
`INGEST_LOG_EVERY` environment variable to a positive integer to emit an INFO
level `ingest_batch` log every N batches. INFO logs are also emitted whenever
new releases are inserted. Setting `INGEST_LOG_EVERY=0` disables periodic INFO
logs.
