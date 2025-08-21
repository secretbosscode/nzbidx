"""Database migration helpers."""

from __future__ import annotations

from typing import Any


def migrate_release_table(conn: Any) -> None:
    """Migrate ``release`` rows into partitioned tables by ``category_id``.

    This function is idempotent.  If the ``release`` table does not exist or is
    already partitioned, no action is taken.
    """

    cur = conn.cursor()

    # Determine whether the table exists and is already partitioned.
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
        return
    _, partrelid = row

    if partrelid is None:
        # Prepare data for partitioning.
        cur.execute(
            "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS category_id INT",
        )
        cur.execute(
            "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ",
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
                LIKE release_old INCLUDING DEFAULTS
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

        # Enforce uniqueness on norm_title/category_id across partitions.
        cur.execute(
            "ALTER TABLE release ADD CONSTRAINT release_norm_title_category_id_key UNIQUE (norm_title, category_id)",
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
        cur.execute(
            "CREATE UNIQUE INDEX release_norm_title_category_id_key ON release (norm_title, category_id)",
        )

        # Copy rows and drop the old table.
        cur.execute("INSERT INTO release SELECT * FROM release_old")
        cur.execute("DROP TABLE release_old")

        conn.commit()

    _ensure_time_partitions(conn)


def _ensure_time_partitions(conn: Any) -> None:
    """Ensure category partitions are subpartitioned by posted_at."""

    cur = conn.cursor()
    categories = {
        "movies": "FOR VALUES FROM (2000) TO (3000)",
        "music": "FOR VALUES FROM (3000) TO (4000)",
        "tv": "FOR VALUES FROM (5000) TO (6000)",
        "adult": "FOR VALUES FROM (6000) TO (7000)",
        "books": "FOR VALUES FROM (7000) TO (8000)",
        "other": "DEFAULT",
    }
    for name, range_sql in categories.items():
        cur.execute("SELECT 1 FROM pg_class WHERE relname = %s", (f"release_{name}",))
        if cur.fetchone() is None:
            continue
        cur.execute(
            "SELECT 1 FROM pg_partitioned_table WHERE partrelid = %s::regclass",
            (f"release_{name}",),
        )
        if cur.fetchone():
            continue
        cur.execute(f"ALTER TABLE release DETACH PARTITION release_{name}")
        cur.execute(f"ALTER TABLE release_{name} RENAME TO release_{name}_old")
        cur.execute(
            f"""CREATE TABLE release_{name} PARTITION OF release {range_sql} PARTITION BY RANGE (posted_at)"""
        )
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS release_{name}_2024 PARTITION OF release_{name} FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"""
        )
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS release_{name}_default PARTITION OF release_{name} DEFAULT"""
        )
        cur.execute(f"INSERT INTO release_{name} SELECT * FROM release_{name}_old")
        cur.execute(f"DROP TABLE release_{name}_old")
    conn.commit()


def add_release_has_parts_index(conn: Any) -> None:
    """Create partial index on ``release`` rows that have parts."""

    cur = conn.cursor()
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_has_parts_idx ON release (id) WHERE has_parts",
    )
    conn.commit()
