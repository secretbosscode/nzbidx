# NZBidx API

## Schema Initialization

The API loads `schema.sql` on startup. When available, the optional
`sqlparse` module splits the file into individual statements. If `sqlparse`
is absent, a built-in splitter handles common PostgreSQL quoting constructs
such as single quotes and dollar-quoted function bodies.

## NNTP Group Selection and Timeouts

`build_nzb_for_release` connects to the configured NNTP server and searches for
articles related to a given release. The API **requires** `NNTP_GROUPS` to be
set either to a comma-separated list or a wildcard pattern such as
`alt.binaries.*`. Wildcards trigger `server.list` enumeration;
`NNTP_GROUP_LIMIT` caps the number of groups scanned. On servers hosting many
groups this enumeration can exceed the default 600‑second total timeout.

Unlike the ingest worker, the API does not yet auto-discover groups. If
`NNTP_GROUPS` is unset or empty the API will fail startup validation.
Autodiscovery via `NNTP_GROUP_WILDCARD` currently applies **only** to the
ingest worker.

Set `NNTP_GROUPS` to a curated list to avoid scanning the entire server.
Alternatively, increase `NNTP_TOTAL_TIMEOUT` and `NZB_TIMEOUT_SECONDS` to allow
more time for the NZB build to complete.

### Docker Compose Examples

Explicit group list for the API service:

```yaml
services:
  nzbidx_api:
    environment:
      NNTP_GROUPS: "alt.binaries.example,alt.binaries.other"
```

Autodiscovery (ingest worker only):

```yaml
services:
  nzbidx_ingest:
    environment:
      NNTP_GROUP_WILDCARD: "alt.binaries.*"  # enumerates groups at runtime
      NNTP_GROUP_LIMIT: 1000                 # optional safety cap
```

Once autodiscovery is supported by the API, the same configuration can be
applied to the `nzbidx_api` service.

## Diagnostics

Use `/api/config` to retrieve the effective `NNTP_TOTAL_TIMEOUT` and
`NZB_TIMEOUT_SECONDS` values for troubleshooting.

## Search Requirements and Backfilling

The `/api/search` endpoint only returns rows where `has_parts` is set. If a
release was ingested without segment metadata it will be hidden from search
results until the segments are populated and `has_parts` becomes `TRUE`.

Run the helper to populate missing segments:

```
docker compose exec nzbidx python scripts/backfill_release_parts.py --auto
```

The script scans the `release` table for entries marked with `has_parts` but
missing `segments`, fills in the `segments` and `part_count` columns when
possible, and deletes releases that can no longer be retrieved.

To verify the flag, inspect the `release` table directly:

```
SELECT id, has_parts, part_count FROM release WHERE has_parts = TRUE;
```


