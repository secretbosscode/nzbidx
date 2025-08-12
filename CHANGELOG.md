# Changelog

## [Unreleased]

- Upgrade setuptools to 78.1.1 to resolve security advisories.
- Restrict backend services from exposing ports; only the API on port 8080 is published by default.
- Add admin takedown endpoint for removing releases from the search index.
- Default to SafeSearch off and enable XXX content unless explicitly disabled.
- Bundle nntplib from CPython and remove its deprecation guard to ensure Python 3.13 compatibility.

## [0.0.0] - 2024-01-01
- Initial release.
