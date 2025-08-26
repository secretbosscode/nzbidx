"""Add descending index on release.posted_at."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Create release_posted_at_idx index."""
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at DESC)"
        )
    finally:
        cur.close()
