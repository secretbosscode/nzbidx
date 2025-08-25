"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Create posted_at indexes concurrently for all partitions."""
    cur = conn.cursor()
    try:
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
            if table == "release":
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table}" (posted_at)'
                )
            else:
                cur.execute(
                    f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" ON "{table}" (posted_at)'
                )
    finally:
        cur.close()
