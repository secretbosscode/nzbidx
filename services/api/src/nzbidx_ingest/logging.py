"""Logging utilities for the ingest worker."""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict
import threading

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


_LOG_LOCK = threading.Lock()


def setup_logging() -> None:
    """Configure root logger for structured JSON output."""
    root = logging.getLogger()
    if getattr(root, "_nzbidx_logging_configured", False):
        return
    with _LOG_LOCK:
        if getattr(root, "_nzbidx_logging_configured", False):
            return

        handler = logging.StreamHandler(sys.stdout)
        log_format = os.getenv("LOG_FORMAT", "plain")
        if log_format.lower() == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )

        root.handlers.clear()
        root.addHandler(handler)
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        root.setLevel(getattr(logging, level, logging.INFO))

        # Quiet overly chatty third‑party libraries to keep logs focused.
        for name in ("urllib3", "opensearchpy", "httpx"):
            logging.getLogger(name).setLevel(logging.WARNING)

        # Ensure uvicorn access logs use the same handler without propagation.
        access = logging.getLogger("uvicorn.access")
        access.handlers.clear()
        access.propagate = False
        access.addHandler(handler)

        root._nzbidx_logging_configured = True
