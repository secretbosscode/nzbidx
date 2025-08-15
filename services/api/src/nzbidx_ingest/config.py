"""Configuration helpers for the ingest worker."""

from __future__ import annotations

import logging
import os
from typing import List

from .nntp_client import NNTPClient

logger = logging.getLogger(__name__)


def _load_groups() -> List[str]:
    env = os.getenv("NNTP_GROUPS", "")
    if env:
        groups = [g.strip() for g in env.split(",") if g.strip()]
        logger.info(
            "Using configured NNTP groups: %s",
            groups,
            extra={"event": "ingest_groups_config", "groups": groups},
        )
        return groups

    client = NNTPClient()
    groups = client.list_groups()
    if groups:
        logger.info(
            "Discovered %d NNTP groups from server",
            len(groups),
            extra={"event": "ingest_groups_discovered", "groups": groups},
        )
    else:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        logger.warning("ingest_groups_missing", extra={"host": host})
    return groups


NNTP_GROUPS: List[str] = _load_groups()


def _load_ignore_groups() -> List[str]:
    env = os.getenv("NNTP_IGNORE_GROUPS", "")
    if env:
        groups = [g.strip() for g in env.split(",") if g.strip()]
        logger.info(
            "Ignoring NNTP groups per configuration: %s",
            groups,
            extra={"event": "ingest_ignore_groups_config", "groups": groups},
        )
        return groups
    return []


IGNORE_GROUPS: List[str] = _load_ignore_groups()
# Benchmarks showed a batch size of 1000 with a polling interval between
# 5s and 60s provided the best ingest throughput without increasing load.
INGEST_BATCH: int = int(os.getenv("INGEST_BATCH", "1000"))
# Dynamic batch sizing is bounded by configurable minimum/maximum limits.
INGEST_BATCH_MIN: int = int(os.getenv("INGEST_BATCH_MIN", "100"))
INGEST_BATCH_MAX: int = int(os.getenv("INGEST_BATCH_MAX", str(INGEST_BATCH)))
INGEST_POLL_MIN_SECONDS: int = int(os.getenv("INGEST_POLL_MIN_SECONDS", "5"))
INGEST_POLL_MAX_SECONDS: int = int(os.getenv("INGEST_POLL_MAX_SECONDS", "60"))
DETECT_LANGUAGE: int = int(os.getenv("DETECT_LANGUAGE", "1"))
CURSOR_DB: str = os.getenv("CURSOR_DB") or os.getenv("DATABASE_URL", "./cursors.sqlite")
CB_RESET_SECONDS: int = int(os.getenv("CB_RESET_SECONDS", "30"))
# Base delay applied when database or OpenSearch latency exceeds
# thresholds. Set to ``0`` to disable adaptive backoff.
INGEST_SLEEP_MS: int = int(os.getenv("INGEST_SLEEP_MS", "1000"))
INGEST_DB_LATENCY_MS: int = int(os.getenv("INGEST_DB_LATENCY_MS", "1200"))
INGEST_OS_LATENCY_MS: int = int(os.getenv("INGEST_OS_LATENCY_MS", "1200"))
INGEST_OS_BULK: int = int(os.getenv("INGEST_OS_BULK", "100"))
# Emit ingest batch metrics at INFO level every N batches. Set to 0 to disable.
INGEST_LOG_EVERY: int = int(os.getenv("INGEST_LOG_EVERY", "100"))


def opensearch_timeout_seconds() -> int:
    """Timeout in seconds for OpenSearch connections."""
    value = os.getenv("OPENSEARCH_TIMEOUT_SECONDS")
    if value is None:
        return 2
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid OPENSEARCH_TIMEOUT_SECONDS=%r, using default 2", value)
        return 2
