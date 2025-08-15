# Logging

The API emits structured logs to aid diagnostics. Timeout errors during NZB
retrieval log a message similar to:

```
"nzb fetch timed out after 30s"
```

Each timeout log includes the `release_id` and `request_id` fields so that
clients can correlate the entry with a specific request.
