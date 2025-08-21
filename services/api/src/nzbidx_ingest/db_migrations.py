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

    # Create partitions with sub-partitioning by posted_at.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release
            FOR VALUES FROM (2000) TO (3000)
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_movies_2024 PARTITION OF release_movies FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_music PARTITION OF release
            FOR VALUES FROM (3000) TO (4000)
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_music_2024 PARTITION OF release_music FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_tv PARTITION OF release
            FOR VALUES FROM (5000) TO (6000)
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_tv_2024 PARTITION OF release_tv FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_adult PARTITION OF release
            FOR VALUES FROM (6000) TO (7000)
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_adult_2024 PARTITION OF release_adult FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_books PARTITION OF release
            FOR VALUES FROM (7000) TO (8000)
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_books_2024 PARTITION OF release_books FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS release_other PARTITION OF release DEFAULT
            PARTITION BY RANGE (posted_at)
        """,
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS release_other_2024 PARTITION OF release_other FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
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
        "CREATE INDEX release_has_parts_idx ON release (posted_at) WHERE has_parts",
    )
    cur.execute(
        "CREATE UNIQUE INDEX release_norm_title_category_id_key ON release (norm_title, category_id)",
    )

    # Copy rows and drop the old table.
    cur.execute("INSERT INTO release SELECT * FROM release_old")
    cur.execute("DROP TABLE release_old")

    conn.commit()


def migrate_release_time_partitions(conn: Any) -> None:
    """Sub-partition existing ``release_*`` tables by ``posted_at`` year."""

    cur = conn.cursor()
    partitions = [
        ("release_movies", "FOR VALUES FROM (2000) TO (3000)"),
        ("release_music", "FOR VALUES FROM (3000) TO (4000)"),
        ("release_tv", "FOR VALUES FROM (5000) TO (6000)"),
        ("release_adult", "FOR VALUES FROM (6000) TO (7000)"),
        ("release_books", "FOR VALUES FROM (7000) TO (8000)"),
        ("release_other", "DEFAULT"),
    ]

    for name, range_spec in partitions:
        cur.execute("SELECT 1 FROM pg_class WHERE relname=%s", (name,))
        if cur.fetchone() is None:
            continue

        cur.execute(
            """
            SELECT 1 FROM pg_partitioned_table p
            JOIN pg_class c ON p.partrelid = c.oid
            WHERE c.relname = %s
            """,
            (name,),
        )
        if cur.fetchone():
            # Already partitioned; ensure 2024 partition exists
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {name}_2024 PARTITION OF {name} FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            )
            continue

        # Detach existing partition and recreate as partitioned by posted_at
        cur.execute(f"ALTER TABLE release DETACH PARTITION {name}")
        cur.execute(f"ALTER TABLE {name} RENAME TO {name}_old")
        if range_spec == "DEFAULT":
            cur.execute(
                f"""
                CREATE TABLE {name} PARTITION OF release DEFAULT
                    PARTITION BY RANGE (posted_at)
                """,
            )
        else:
            cur.execute(
                f"""
                CREATE TABLE {name} PARTITION OF release {range_spec}
                    PARTITION BY RANGE (posted_at)
                """,
            )
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {name}_2024 PARTITION OF {name} FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        )
        cur.execute(f"INSERT INTO {name} SELECT * FROM {name}_old")
        cur.execute(f"DROP TABLE {name}_old")

    conn.commit()
