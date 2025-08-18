#!/usr/bin/env python
"""Migrate ``release`` rows into partitioned tables by ``category_id``."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure local packages are importable when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore  # noqa: E402
from nzbidx_ingest.db_migrations import migrate_release_table  # type: ignore  # noqa: E402


def migrate(conn=None) -> None:
    close = False
    if conn is None:
        conn = connect_db()
        close = True
    migrate_release_table(conn)
    if close:
        conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
