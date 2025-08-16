# Logging

The API emits plain text logs by default. Set `LOG_FORMAT=json` to switch to
structured JSON logs that aid diagnostics. Timeout errors during NZB retrieval
log a message similar to:

```
"nzb fetch timed out after 30s"
```

Each timeout log includes the `release_id` and `request_id` fields so that
clients can correlate the entry with a specific request.

## Ingest Metrics

The ingestion loop logs a summary after processing each NNTP group. Fields
included in the log are described below.

| Field | Description |
| --- | --- |
| `processed` | Headers processed in the batch. |
| `inserted` | Releases written to the database. |
| `indexed` | Releases indexed into OpenSearch. |
| `deduplicated` | Releases skipped due to duplicate detection. |
| `duration_ms` | Total processing time for the batch. |
| `average_batch_ms` | Average processing time per header. |
| `opensearch_latency_ms` | Time spent bulk indexing into OpenSearch. |
| `average_database_latency_ms` | Average database write latency per header. |
| `average_opensearch_latency_ms` | Average OpenSearch indexing latency per document. |
| `cursor` | Last article number processed. |
| `high_water` | Highest article number available on the server. |
| `remaining` | Articles still pending processing. |
| `percent_complete` | Progress relative to the high-water mark. |
| `eta_seconds` | Estimated time to ingest remaining articles. |
| `group` | NNTP group associated with the batch. |
