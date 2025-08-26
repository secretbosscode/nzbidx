"""Utilities for scrubbing sensitive data from logs."""

from __future__ import annotations

import logging
from typing import Mapping

SENSITIVE_HEADERS = {"authorization", "x-api-key", "cookie"}


def scrub_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        k: ("[redacted]" if k.lower() in SENSITIVE_HEADERS else v)
        for k, v in headers.items()
    }


class LogSanitizerFilter(logging.Filter):
    """Logging filter that redacts sensitive headers and long queries."""

    __slots__ = ()

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        headers = getattr(record, "headers", None)
        if isinstance(headers, Mapping):
            record.headers = scrub_headers(headers)
        query = getattr(record, "query", None)
        if isinstance(query, str) and len(query) > 256:
            record.query = query[:256] + "…"
        return True
