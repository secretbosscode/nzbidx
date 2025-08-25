"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Create posted_at indexes concurrently for all partitions."""
    cur = conn.cursor()
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block
    autocommit = getattr(conn, "autocommit", False)
    try:
        if hasattr(conn, "autocommit"):
            conn.autocommit = True
        cur.execute(
            """
            SELECT inhrelid::regclass::text
            FROM pg_inherits
            WHERE inhparent = 'release'::regclass
            """
        )
        tables = ["release"] + [row[0] for row in cur.fetchall()]
        for table in tables:
            index_name = f"{table.replace('.', '_')}_posted_at_idx"
            cur.execute(
                f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" ON "{table}" (posted_at)'
            )
    finally:
        cur.close()
        if hasattr(conn, "autocommit"):
            conn.autocommit = autocommit
    if not getattr(conn, "autocommit", False):
        conn.commit()
