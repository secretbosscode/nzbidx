# NZBidx API

## NNTP Group Selection and Timeouts

`build_nzb_for_release` connects to the configured NNTP server and searches for
articles related to a given release. If the `NNTP_GROUPS` environment variable
is **unset**, the builder retrieves the full group list via `server.list()` and
iterates over every group. On servers hosting many groups this enumeration can
exceed the default 60‑second total timeout.

Set `NNTP_GROUPS` to the relevant groups to avoid scanning the entire server.
Alternatively, increase `NNTP_TOTAL_TIMEOUT` and `NZB_TIMEOUT_SECONDS` to allow
more time for the NZB build to complete.

## Diagnostics

Use `/api/config` to retrieve the effective `NNTP_TOTAL_TIMEOUT` and
`NZB_TIMEOUT_SECONDS` values for troubleshooting.

