# Logging

The API emits human‑readable messages combined with structured fields to aid
diagnostics. Timeout errors during NZB retrieval log a message similar to:

```
nzb fetch timed out after 30s
```

Each timeout log includes the `release_id` and `request_id` fields so that
clients can correlate the entry with a specific request.

### Example

The ingest worker records per‑batch progress using descriptive sentences while
still providing machine‑parsable fields:

```
Processed 50 articles for alt.binaries.example: 20 inserted, 30 duplicates; 100 remaining (67% done)
{
  'event': 'ingest_batch',
  'group': 'alt.binaries.example',
  'processed': 50,
  'inserted': 20,
  'deduped': 30,
  'remaining': 100,
  'pct_complete': 67
}
```

The text portion is intended for humans. The accompanying dictionary contains
stable keys such as `event` for downstream parsing and analytics.
