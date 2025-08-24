"""Add search vector column and index."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Add ``search_vector`` column and its index."""
    cur = conn.cursor()
    cur.execute(
        """
        ALTER TABLE release
        ADD COLUMN IF NOT EXISTS search_vector tsvector
            GENERATED ALWAYS AS (
                to_tsvector('simple', coalesce(norm_title,'') || ' ' || coalesce(tags,''))
            ) STORED
        """
    )
    conn.commit()
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block
    autocommit = getattr(conn, "autocommit", False)
    try:
        if hasattr(conn, "autocommit"):
            conn.autocommit = True
        cur.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS release_search_idx
            ON release USING GIN (search_vector)
            """
        )
    finally:
        if hasattr(conn, "autocommit"):
            conn.autocommit = autocommit
    if not getattr(conn, "autocommit", False):
        conn.commit()


if __name__ == "__main__":  # pragma: no cover - script entry
    import os

    try:
        import psycopg
    except Exception:  # pragma: no cover - dependency check
        raise SystemExit("psycopg is required to run this migration")

    conn = psycopg.connect(os.environ.get("DATABASE_URL", ""))
    try:
        migrate(conn)
    finally:
        conn.close()
