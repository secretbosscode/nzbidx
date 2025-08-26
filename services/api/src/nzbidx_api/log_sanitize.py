"""Utilities for scrubbing sensitive data from logs."""

from __future__ import annotations

import logging
from typing import Mapping

SENSITIVE_HEADERS = {"authorization", "x-api-key", "cookie"}


def scrub_headers(headers: Mapping[str, str]) -> Mapping[str, str]:
    """Redact sensitive header values.

    A new ``dict`` is only created when at least one sensitive header is present.
    Otherwise the original mapping is returned unchanged.
    """

    if not any(k.lower() in SENSITIVE_HEADERS for k in headers):
        return headers

    redacted = dict(headers)
    for k in headers:
        if k.lower() in SENSITIVE_HEADERS:
            redacted[k] = "[redacted]"
    return redacted


class LogSanitizerFilter(logging.Filter):
    """Logging filter that redacts sensitive headers and long queries."""

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        headers = getattr(record, "headers", None)
        if isinstance(headers, Mapping):
            record.headers = scrub_headers(headers)
        query = getattr(record, "query", None)
        if isinstance(query, str) and len(query) > 256:
            record.query = query[:256] + "…"
        return True
