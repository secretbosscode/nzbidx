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
        "CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release FOR VALUES FROM (6000) TO (7000) PARTITION BY RANGE (posted_at)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult_2024 PARTITION OF release_adult FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult_default PARTITION OF release_adult DEFAULT",
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


def migrate_release_adult_partitions(conn: Any, batch_size: int = 1000) -> None:
    """Ensure ``release_adult`` is partitioned by ``posted_at`` and migrate rows.

    The existing ``release_adult`` partition is converted into a partitioned table
    with yearly child partitions. Rows are moved in batches to avoid long locks.
    """

    cur = conn.cursor()

    # Check if ``release_adult`` is already partitioned by ``posted_at``.
    cur.execute(
        "SELECT partrelid FROM pg_partitioned_table WHERE partrelid = 'release_adult'::regclass"
    )
    if cur.fetchone() is not None:
        return

    # Detach and rename existing partition.
    cur.execute("ALTER TABLE release DETACH PARTITION release_adult")
    cur.execute("ALTER TABLE release_adult RENAME TO release_adult_old")

    # Create new partitioned table and initial partitions.
    cur.execute(
        "CREATE TABLE release_adult PARTITION OF release FOR VALUES FROM (6000) TO (7000) PARTITION BY RANGE (posted_at)"
    )

    # Determine years present in existing data.
    cur.execute(
        "SELECT DISTINCT EXTRACT(YEAR FROM posted_at) FROM release_adult_old WHERE posted_at IS NOT NULL"
    )
    years = [int(row[0]) for row in cur.fetchall()]
    for year in years:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS release_adult_{year} PARTITION OF release_adult FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')"
        )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult_default PARTITION OF release_adult DEFAULT"
    )
    conn.commit()

    # Copy rows in batches to new partitioned table.
    while True:
        cur.execute(
            f"""
            WITH moved AS (
                SELECT * FROM release_adult_old ORDER BY id LIMIT {batch_size}
            )
            INSERT INTO release_adult SELECT * FROM moved RETURNING id
            """
        )
        ids = [row[0] for row in cur.fetchall()]
        if not ids:
            break
        cur.execute("DELETE FROM release_adult_old WHERE id = ANY(%s)", (ids,))
        conn.commit()

    cur.execute("DROP TABLE release_adult_old")
    conn.commit()


def ensure_release_adult_year_partition(conn: Any, year: int) -> None:
    """Create a yearly ``release_adult`` partition if it does not exist."""

    table = f"release_adult_{year}"
    cur = conn.cursor()
    cur.execute("SELECT to_regclass(%s)", (table,))
    if cur.fetchone()[0] is not None:
        return
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table} PARTITION OF release_adult FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')"
    )
    conn.commit()


def add_release_has_parts_index(conn: Any) -> None:
    """Create partial index on ``release`` rows that have parts."""

    cur = conn.cursor()
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_has_parts_idx ON release (id) WHERE has_parts",
    )
    conn.commit()
