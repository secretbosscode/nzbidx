"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any

from psycopg import sql


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
            cur.execute(
                sql.SQL(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (posted_at)"
                ).format(sql.Identifier(index_name), sql.Identifier(table))
            )
    finally:
        cur.close()
    conn.commit()
