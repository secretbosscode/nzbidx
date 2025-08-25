"""Add index on release.posted_at."""

from __future__ import annotations

from typing import Any


def migrate(conn: Any) -> None:
    """Create posted_at indexes for all partitions.

    Leaf partitions receive a concurrent index build while partitioned tables
    use plain ``CREATE INDEX`` so that indexes are attached recursively.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT relid::regclass::text, isleaf
            FROM pg_partition_tree('release')
            ORDER BY level DESC
            """
        )
        for table, is_leaf in cur.fetchall():
            index_name = f"{table.replace('.', '_')}_posted_at_idx"
            if is_leaf:
                cur.execute(
                    f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" ON "{table}" (posted_at)'
                )
            else:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table}" (posted_at)'
                )
    finally:
        cur.close()
