"""Configuration helpers for the ingest service."""

from __future__ import annotations

import os
from typing import List

NNTP_GROUPS: List[str] = [
    g.strip() for g in os.getenv("NNTP_GROUPS", "").split(",") if g.strip()
]
INGEST_BATCH: int = int(os.getenv("INGEST_BATCH", "500"))
INGEST_POLL_SECONDS: int = int(os.getenv("INGEST_POLL_SECONDS", "60"))
CURSOR_DB: str = os.getenv("CURSOR_DB", "./cursors.sqlite")
CB_RESET_SECONDS: int = int(os.getenv("CB_RESET_SECONDS", "30"))
INGEST_OS_LATENCY_MS: int = int(os.getenv("INGEST_OS_LATENCY_MS", "1200"))
