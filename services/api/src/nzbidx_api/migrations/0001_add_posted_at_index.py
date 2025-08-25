"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any


def _quote_ident(name: str) -> str:
    """Return ``name`` quoted as an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def migrate(conn: Any) -> None:
    """Create release_posted_at_idx concurrently for all partitions."""
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
            table_ident = _quote_ident(table)
            cur.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"release_posted_at_idx ON {table_ident} (posted_at)"
            )
    finally:
        cur.close()
        if hasattr(conn, "autocommit"):
            conn.autocommit = autocommit
    if not getattr(conn, "autocommit", False):
        conn.commit()
