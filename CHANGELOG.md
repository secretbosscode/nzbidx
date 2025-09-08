# Changelog

## [Unreleased]

- Upgrade setuptools to 78.1.1 to resolve security advisories.
- Restrict backend services from exposing ports; only the API on port 8080 is published by default.
- Add admin takedown endpoint for removing releases from the search index.
- Default to SafeSearch off and enable XXX content unless explicitly disabled.
- Depend on the `standard-nntplib` package to retain NNTP support after its removal from the standard library.
- Consolidate ingest into the API application and remove the separate ingest service.
- Ensure release `language` and `tags` columns have non-null defaults.
- Include `langdetect` dependency for automatic language detection.
- Ignore empty search queries to avoid returning entire categories.
- Handle search vector migration automatically at startup; manual SQL script removed.
- Add `release_posted_at_idx` index on `posted_at DESC`.
- Add configurable retry attempts and delay for NNTP connections.
- Optionally trust `X-Forwarded-For`/`X-Real-IP` headers for rate limiting via `TRUST_PROXY_HEADERS`.

## [0.0.0] - 2024-01-01
- Initial release.
