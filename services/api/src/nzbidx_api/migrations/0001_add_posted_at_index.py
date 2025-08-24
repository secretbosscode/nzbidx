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
            table_ident = sql.Identifier(table).as_string(conn)
            cur.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"release_posted_at_idx ON {table_ident} (posted_at)"
            )
    finally:
        cur.close()
    conn.commit()
