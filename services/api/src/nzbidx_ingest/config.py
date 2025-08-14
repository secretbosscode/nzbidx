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
        logger.info("ingest_groups_config", extra={"groups": groups})
        return groups

    client = NNTPClient()
    groups = client.list_groups()
    if groups:
        logger.info("ingest_groups_discovered", extra={"groups": groups})
    else:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        logger.warning("ingest_groups_missing", extra={"host": host})
    return groups


NNTP_GROUPS: List[str] = _load_groups()


def _load_ignore_groups() -> List[str]:
    env = os.getenv("NNTP_IGNORE_GROUPS", "")
    if env:
        groups = [g.strip() for g in env.split(",") if g.strip()]
        logger.info("ingest_ignore_groups_config", extra={"groups": groups})
        return groups
    return []


IGNORE_GROUPS: List[str] = _load_ignore_groups()
INGEST_BATCH: int = int(os.getenv("INGEST_BATCH", "500"))
INGEST_POLL_SECONDS: int = int(os.getenv("INGEST_POLL_SECONDS", "60"))
CURSOR_DB: str = os.getenv("CURSOR_DB") or os.getenv("DATABASE_URL", "./cursors.sqlite")
CB_RESET_SECONDS: int = int(os.getenv("CB_RESET_SECONDS", "30"))
INGEST_SLEEP_MS: int = int(os.getenv("INGEST_SLEEP_MS", "0"))
INGEST_DB_LATENCY_MS: int = int(os.getenv("INGEST_DB_LATENCY_MS", "1200"))
INGEST_OS_LATENCY_MS: int = int(os.getenv("INGEST_OS_LATENCY_MS", "1200"))
INGEST_OS_BULK: int = int(os.getenv("INGEST_OS_BULK", "100"))
