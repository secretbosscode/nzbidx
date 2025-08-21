#!/usr/bin/env python
"""Create the partial ``release_has_parts_idx`` index."""

from __future__ import annotations

import sys
from pathlib import Path


# Ensure local packages are importable when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore  # noqa: E402
from nzbidx_ingest.db_migrations import (  # type: ignore  # noqa: E402
    add_release_has_parts_index,
)


def migrate(conn=None) -> None:
    close = False
    if conn is None:
        conn = connect_db()
        close = True
    add_release_has_parts_index(conn)
    if close:
        conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
