"""Configuration helpers for the ingest service."""

from __future__ import annotations

import os
from typing import List

NNTP_HOST: str | None = os.getenv("NNTP_HOST")
NNTP_PORT: int = int(os.getenv("NNTP_PORT", "119"))
NNTP_USER: str | None = os.getenv("NNTP_USER")
NNTP_PASS: str | None = os.getenv("NNTP_PASS")
NNTP_GROUPS: List[str] = [
    g.strip() for g in os.getenv("NNTP_GROUPS", "").split(",") if g.strip()
]
INGEST_BATCH: int = int(os.getenv("INGEST_BATCH", "500"))
INGEST_POLL_SECONDS: int = int(os.getenv("INGEST_POLL_SECONDS", "60"))
CURSOR_DB: str = os.getenv("CURSOR_DB", "./cursors.sqlite")
ILM_DELETE_DAYS: int = int(os.getenv("ILM_DELETE_DAYS", "180"))
ILM_WARM_DAYS: int = int(os.getenv("ILM_WARM_DAYS", "14"))
CB_RESET_SECONDS: int = int(os.getenv("CB_RESET_SECONDS", "30"))
INGEST_OS_LATENCY_MS: int = int(os.getenv("INGEST_OS_LATENCY_MS", "1200"))
