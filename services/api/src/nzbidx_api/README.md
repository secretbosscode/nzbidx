# NZBidx API

## Schema Initialization

The API loads `schema.sql` on startup. When available, the optional
`sqlparse` module splits the file into individual statements. If `sqlparse`
is absent, a built-in splitter handles common PostgreSQL quoting constructs
such as single quotes and dollar-quoted function bodies.

## NNTP Group Selection and Timeouts

`build_nzb_for_release` connects to the configured NNTP server and searches for
articles related to a given release. `NNTP_GROUPS` must be set to a
comma-separated list or wildcard pattern. Wildcards trigger `server.list`
enumeration; `NNTP_GROUP_LIMIT` caps the number of groups scanned. On servers
hosting many groups this enumeration can exceed the default 600‑second total
timeout.

Set `NNTP_GROUPS` to a curated list to avoid scanning the entire server.
Alternatively, increase `NNTP_TOTAL_TIMEOUT` and `NZB_TIMEOUT_SECONDS` to allow
more time for the NZB build to complete.

## Diagnostics

Use `/api/config` to retrieve the effective `NNTP_TOTAL_TIMEOUT` and
`NZB_TIMEOUT_SECONDS` values for troubleshooting.

