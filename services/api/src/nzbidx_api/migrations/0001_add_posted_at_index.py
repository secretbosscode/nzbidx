"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any

from psycopg import sql


def migrate(conn: Any) -> None:
    """Create release_posted_at_idx concurrently for all partitions."""
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
            cur.execute(
                sql.SQL(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS release_posted_at_idx ON {} (posted_at)"
                ).format(sql.Identifier(table))
            )
    finally:
        cur.close()
    conn.commit()
