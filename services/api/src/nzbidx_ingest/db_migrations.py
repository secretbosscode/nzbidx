"""Database migration helpers."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Iterable


# Category ranges in the ``release`` partitioned table.  The ``other`` category
# acts as the default partition and therefore has no explicit bounds.
CATEGORY_RANGES: dict[str, tuple[int, int] | None] = {
    "movies": (2000, 3000),
    "music": (3000, 4000),
    "tv": (5000, 6000),
    "adult": (6000, 7000),
    "books": (7000, 8000),
    "other": None,
}


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
        "CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release FOR VALUES FROM (2000) TO (3000) PARTITION BY RANGE (posted_at)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_movies_2024 PARTITION OF release_movies FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_movies_default PARTITION OF release_movies DEFAULT",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music PARTITION OF release FOR VALUES FROM (3000) TO (4000) PARTITION BY RANGE (posted_at)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music_2024 PARTITION OF release_music FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music_default PARTITION OF release_music DEFAULT",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release FOR VALUES FROM (5000) TO (6000) PARTITION BY RANGE (posted_at)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv_2024 PARTITION OF release_tv FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv_default PARTITION OF release_tv DEFAULT",
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
        "CREATE TABLE IF NOT EXISTS release_books PARTITION OF release FOR VALUES FROM (7000) TO (8000) PARTITION BY RANGE (posted_at)",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_books_2024 PARTITION OF release_books FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_books_default PARTITION OF release_books DEFAULT",
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

    create_release_posted_at_index(conn)

    # Copy rows and drop the old table.
    cur.execute("INSERT INTO release SELECT * FROM release_old")
    cur.execute("DROP TABLE release_old")

    conn.commit()


def migrate_release_partitions_by_date(
    conn: Any, category: str, batch_size: int = 1000
) -> None:
    """Ensure ``release_<category>`` is partitioned by ``posted_at``.

    The function converts the existing category partition into a partitioned
    table by ``posted_at`` and migrates any existing rows into yearly child
    partitions.  It is safe to call multiple times.
    """

    table = f"release_{category}"
    ranges = CATEGORY_RANGES.get(category)
    if category not in CATEGORY_RANGES:
        raise ValueError(f"unknown category: {category}")

    cur = conn.cursor()

    # Skip if already partitioned by ``posted_at``
    cur.execute(
        "SELECT partrelid FROM pg_partitioned_table WHERE partrelid = %s::regclass",
        (table,),
    )
    if cur.fetchone() is not None:
        return

    # Detach and rename existing partition
    cur.execute(f"ALTER TABLE release DETACH PARTITION {table}")
    cur.execute(f"ALTER TABLE {table} RENAME TO {table}_old")

    # Create new partitioned table with the appropriate bounds
    if ranges is None:
        cur.execute(
            f"CREATE TABLE {table} PARTITION OF release DEFAULT PARTITION BY RANGE (posted_at)"
        )
    else:
        start, end = ranges
        cur.execute(
            f"CREATE TABLE {table} PARTITION OF release FOR VALUES FROM (%s) TO (%s) PARTITION BY RANGE (posted_at)",
            (start, end),
        )

    # Determine distinct years present in existing data
    cur.execute(
        f"SELECT DISTINCT EXTRACT(YEAR FROM posted_at) FROM {table}_old WHERE posted_at IS NOT NULL"
    )
    years = [int(r[0]) for r in cur.fetchall()]
    for year in years:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table}_{year} PARTITION OF {table} FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')"
        )
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table}_default PARTITION OF {table} DEFAULT"
    )
    conn.commit()
    create_release_posted_at_index(conn)

    # Move rows into new partitioned table
    while True:
        cur.execute(
            f"""
            WITH moved AS (
                SELECT * FROM {table}_old ORDER BY id LIMIT {batch_size}
            )
            INSERT INTO {table} SELECT * FROM moved RETURNING id
            """
        )
        ids = [row[0] for row in cur.fetchall()]
        if not ids:
            break
        cur.execute(f"DELETE FROM {table}_old WHERE id = ANY(%s)", (ids,))
        conn.commit()

    cur.execute(f"DROP TABLE {table}_old")
    conn.commit()


def ensure_release_year_partition(conn: Any, category: str, year: int) -> None:
    """Create a yearly ``release_<category>`` partition if it does not exist."""

    table = f"release_{category}_{year}"
    parent = f"release_{category}"
    cur = conn.cursor()
    cur.execute("SELECT to_regclass(%s)", (table,))
    if cur.fetchone()[0] is not None:
        return
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table} PARTITION OF {parent} FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')",
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table}_posted_at_idx ON {table} (posted_at)",
    )
    conn.commit()


def ensure_current_and_next_year_partitions(conn: Any) -> None:
    """Pre-create ``release`` partitions for the current and next year."""

    year = datetime.now().year
    for category, bounds in CATEGORY_RANGES.items():
        if bounds is None:
            continue
        ensure_release_year_partition(conn, category, year)
        ensure_release_year_partition(conn, category, year + 1)


def drop_unused_release_partitions(
    conn: Any, category: str, retain: Iterable[str] | None = None
) -> None:
    """Drop empty ``release_<category>`` partitions not in ``retain``."""

    parent = f"release_{category}"
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = %s
        """,
        (parent,),
    )
    partitions = [row[0] for row in cur.fetchall()]

    if retain is None:
        env = os.getenv(f"RELEASE_{category.upper()}_PARTITIONS_RETAIN", "")
        retain_set = {p.strip() for p in env.split(",") if p.strip()}
    else:
        retain_set = set(retain)
    retain_set.add(f"{parent}_default")

    for table in partitions:
        if table in retain_set:
            continue
        cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        if cur.fetchone() is None:
            cur.execute(f"DROP TABLE {table}")
    conn.commit()


# Backward-compatible aliases -------------------------------------------------


def migrate_release_adult_partitions(conn: Any, batch_size: int = 1000) -> None:
    migrate_release_partitions_by_date(conn, "adult", batch_size=batch_size)


def add_release_has_parts_index(conn: Any) -> None:
    """Create partial index on ``release`` rows that have parts."""

    cur = conn.cursor()
    cur.execute(
        "CREATE INDEX IF NOT EXISTS release_has_parts_idx ON release (id) WHERE has_parts",
    )
    conn.commit()


def create_release_posted_at_index(conn: Any) -> None:
    """Ensure ``release_posted_at_idx`` exists on ``release`` and its partitions."""

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
        seen: set[str] = set()
        while tables:
            table = tables.pop()
            if table in seen:
                continue
            seen.add(table)
            index = (
                "release_posted_at_idx"
                if table == "release"
                else f"{table}_posted_at_idx"
            )
            cur.execute(f"CREATE INDEX IF NOT EXISTS {index} ON {table} (posted_at)")
            cur.execute(
                """
                SELECT inhrelid::regclass::text
                FROM pg_inherits
                WHERE inhparent = %s::regclass
                """,
                (table,),
            )
            tables.extend(row[0] for row in cur.fetchall())
    except Exception:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at)",
        )
    conn.commit()
