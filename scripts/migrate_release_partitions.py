#!/usr/bin/env python
"""Migrate release rows into partitioned tables by category_id."""

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
    cur.execute(
        "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category_id INT"
    )
    cur.execute(
        "UPDATE release SET category_id = NULLIF(category, '')::INT "
        "WHERE category_id IS NULL AND category ~ '^[0-9]+'"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_new (
            LIKE release INCLUDING ALL
        ) PARTITION BY RANGE (category_id)
        """
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release_new FOR VALUES FROM (2000) TO (3000)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music PARTITION OF release_new FOR VALUES FROM (3000) TO (4000)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release_new FOR VALUES FROM (5000) TO (6000)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release_new FOR VALUES FROM (6000) TO (7000)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_books PARTITION OF release_new FOR VALUES FROM (7000) TO (8000)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_other PARTITION OF release_new DEFAULT"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_new_norm_title_idx ON release_new USING GIN (norm_title gin_trgm_ops)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_new_tags_idx ON release_new USING GIN (tags gin_trgm_ops)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_new_posted_at_idx ON release_new (posted_at)"
    )
    cur.execute("INSERT INTO release_new SELECT * FROM release")
    cur.execute("ALTER TABLE release RENAME TO release_old")
    cur.execute("ALTER TABLE release_new RENAME TO release")
    conn.commit()
    conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
