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
    if partrelid is not None:
        _migrate_release_adult(conn)
        return

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
        """
        CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release
            FOR VALUES FROM (6000) TO (7000)
            PARTITION BY RANGE (posted_at)
        """
    )

    # Create yearly partitions for existing adult rows.
    _migrate_release_adult(conn, source_table="release_old")
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


def _migrate_release_adult(conn: Any, source_table: str = "release_adult") -> None:
    """Ensure ``release_adult`` is partitioned by ``posted_at`` and populated."""
    cur = conn.cursor()
    cur.execute(
        "SELECT partrelid FROM pg_partitioned_table WHERE partrelid='release_adult'::regclass"
    )
    if cur.fetchone():
        return

    cur.execute("ALTER TABLE release_adult RENAME TO release_adult_old")
    cur.execute(
        """
        CREATE TABLE release_adult PARTITION OF release
            FOR VALUES FROM (6000) TO (7000)
            PARTITION BY RANGE (posted_at)
        """
    )
    cur.execute(
        f"SELECT DISTINCT EXTRACT(YEAR FROM posted_at) FROM {source_table} WHERE posted_at IS NOT NULL"
    )
    for row in cur.fetchall():
        year = int(row[0])
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS release_adult_{year} PARTITION OF release_adult
                FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')
            """
        )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult_default PARTITION OF release_adult DEFAULT",
    )
    cur.execute("INSERT INTO release_adult SELECT * FROM release_adult_old")
    cur.execute("DROP TABLE release_adult_old")
    conn.commit()
