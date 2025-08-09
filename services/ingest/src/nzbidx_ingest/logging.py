"""Logging utilities for ingest service."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict

_LOG_RECORD_DEFAULTS = logging.LogRecord(
    name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
).__dict__.keys()


class JsonFormatter(logging.Formatter):
    """A minimal JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        extras = {
            k: v for k, v in record.__dict__.items() if k not in _LOG_RECORD_DEFAULTS
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        payload.update(extras)
        return json.dumps(payload)


def setup_logging() -> None:
    """Configure root logger for structured JSON output."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
