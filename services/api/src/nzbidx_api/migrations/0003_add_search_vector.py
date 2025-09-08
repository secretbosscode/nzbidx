"""Add search_vector column and GIN index."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Add search_vector column and index if missing."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            ALTER TABLE IF EXISTS release
                ADD COLUMN IF NOT EXISTS search_vector tsvector
                    GENERATED ALWAYS AS (
                        to_tsvector('simple', coalesce(norm_title,'') || ' ' || coalesce(tags,''))
                    ) STORED
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS release_search_idx ON release USING GIN (search_vector)"
        )
    finally:
        cur.close()
