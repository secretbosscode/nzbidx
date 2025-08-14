# Changelog

## [Unreleased]

- Upgrade setuptools to 78.1.1 to resolve security advisories.
- Restrict backend services from exposing ports; only the API on port 8080 is published by default.
- Add admin takedown endpoint for removing releases from the search index.
- Default to SafeSearch off and enable XXX content unless explicitly disabled.
- Bundle nntplib from CPython and remove its deprecation guard to ensure Python 3.13 compatibility.
- Handle OpenSearch clusters without Index Lifecycle Management by skipping ILM configuration.
- Consolidate ingest into the API application and remove the separate ingest service.
- Ensure release `language` and `tags` columns have non-null defaults.
- Include `langdetect` dependency for automatic language detection.
- Ignore empty search queries to avoid returning entire categories.

## [0.0.0] - 2024-01-01
- Initial release.
