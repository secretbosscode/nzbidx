#!/usr/bin/env python
"""Add the ``release_posted_at_idx`` index for existing databases."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure local packages are importable when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore  # noqa: E402
from nzbidx_ingest.db_migrations import (  # type: ignore  # noqa: E402
    create_release_posted_at_index,
)


def migrate(conn=None) -> None:
    close = False
    if conn is None:
        conn = connect_db()
        close = True
    create_release_posted_at_index(conn)
    if close:
        conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
