#!/usr/bin/env python
"""Migrate ``release`` rows into partitioned tables by ``category_id``.

This script is idempotent. It first checks whether the ``release`` table is
already partitioned by consulting ``pg_partitioned_table``. If no partitions are
configured, the existing table is renamed to ``release_old`` and a new
partitioned ``release`` table is created. Data is copied from ``release_old``
into the new table before dropping the old table.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure local packages are importable when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore


def migrate() -> None:
    conn = connect_db()
    cur = conn.cursor()

    # Does an unpartitioned ``release`` table exist?
    cur.execute(
        """
        SELECT c.oid, pt.partrelid
        FROM pg_class c
        LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
        WHERE c.relname = 'release'
        """
    )
    row = cur.fetchone()
    if row is None:
        conn.close()
        return
    _, partrelid = row
    if partrelid is not None:
        conn.close()
        return

    # Prepare data for partitioning.
    cur.execute(
        "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category_id INT",
    )
    cur.execute(
        "UPDATE release SET category_id = NULLIF(category, '')::INT "
        "WHERE category_id IS NULL AND category ~ '^[0-9]+'",
    )

    # Rename existing table and create partitioned replacement.
    cur.execute("ALTER TABLE release RENAME TO release_old")
    cur.execute(
        """
        CREATE TABLE release (
            LIKE release_old INCLUDING DEFAULTS INCLUDING CONSTRAINTS
            INCLUDING STORAGE INCLUDING COMMENTS
        ) PARTITION BY RANGE (category_id)
        """
    )

    # Create partitions.
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release FOR VALUES FROM (2000) TO (3000)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music PARTITION OF release FOR VALUES FROM (3000) TO (4000)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release FOR VALUES FROM (5000) TO (6000)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release FOR VALUES FROM (6000) TO (7000)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_books PARTITION OF release FOR VALUES FROM (7000) TO (8000)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_other PARTITION OF release DEFAULT",
    )

    # Recreate indexes on the new table.
    cur.execute(
        "CREATE INDEX release_norm_title_idx ON release USING GIN (norm_title gin_trgm_ops)",
    )
    cur.execute(
        "CREATE INDEX release_tags_idx ON release USING GIN (tags gin_trgm_ops)",
    )
    cur.execute(
        "CREATE INDEX release_posted_at_idx ON release (posted_at)",
    )

    # Copy rows and drop the old table.
    cur.execute("INSERT INTO release SELECT * FROM release_old")
    cur.execute("DROP TABLE release_old")

    conn.commit()
    conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
