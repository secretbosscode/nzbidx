"""Configuration helpers for the ingest worker."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class NNTPSettings:
    """Connection parameters for an NNTP server."""

    host: str | None
    port: int
    use_ssl: bool
    user: str | None
    password: str | None


def nntp_settings() -> NNTPSettings:
    """Return NNTP connection settings loaded from the environment."""

    host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
    port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
    ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
    use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563
    return NNTPSettings(
        host=host,
        port=port,
        use_ssl=use_ssl,
        user=os.getenv("NNTP_USER"),
        password=os.getenv("NNTP_PASS"),
    )


NNTP_SETTINGS: NNTPSettings = nntp_settings()


from .nntp_client import NNTPClient  # noqa: E402


NNTP_GROUP_WILDCARD: str = os.getenv("NNTP_GROUP_WILDCARD", "alt.binaries.*")


def _load_groups() -> List[str]:
    env = os.getenv("NNTP_GROUPS", "")
    if not env:
        cfg = os.getenv("NNTP_GROUP_FILE")
        if cfg:
            try:
                env = Path(cfg).read_text(encoding="utf-8")
            except OSError:
                env = ""
    if env:
        parts = re.split(r"[\n,]", env)
        groups = [g.strip() for g in parts if g.strip()]
        logger.info(
            "Using configured NNTP groups: %s",
            groups,
            extra={"event": "ingest_groups_config", "groups": groups},
        )
        return groups

    client = NNTPClient(NNTP_SETTINGS)
    groups = client.list_groups(NNTP_GROUP_WILDCARD)
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
# Base delay applied when database latency exceeds thresholds. Set to ``0`` to
# disable adaptive backoff.
INGEST_SLEEP_MS: int = int(os.getenv("INGEST_SLEEP_MS", "1000"))
INGEST_DB_LATENCY_MS: int = int(os.getenv("INGEST_DB_LATENCY_MS", "1200"))
# Emit ingest batch metrics at INFO level every N batches. Set to 0 to disable.
INGEST_LOG_EVERY: int = int(os.getenv("INGEST_LOG_EVERY", "100"))
RELEASE_PART_MAX_RELEASES: int = int(os.getenv("RELEASE_PART_MAX_RELEASES", "100000"))
# Enable strict segment schema validation when set to a truthy value.
# Disabled by default to reduce ingest overhead in production.
VALIDATE_SEGMENTS: bool = os.getenv("VALIDATE_SEGMENTS", "").lower() in {
    "1",
    "true",
    "yes",
}
