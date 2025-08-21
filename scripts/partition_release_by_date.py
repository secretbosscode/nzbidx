#!/usr/bin/env python
"""Split category partitions into yearly partitions based on ``posted_at``."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore  # noqa: E402

CATEGORIES = ["movies", "music", "tv", "adult", "books", "other"]


def ensure_year_partition(cur: Any, category: str, year: int) -> None:
    table = f"release_{category}"
    child = f"{table}_{year}"
    start, end = f"{year}-01-01", f"{year + 1}-01-01"
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {child} PARTITION OF {table}
        FOR VALUES FROM (%s) TO (%s)
        """,
        (start, end),
    )
    cur.execute(
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {child}_posted_at_idx ON {child} (posted_at)"
    )


def move_rows(cur: Any, category: str, year: int, batch: int = 1000) -> None:
    table = f"release_{category}"
    child = f"{table}_{year}"
    start, end = f"{year}-01-01", f"{year + 1}-01-01"
    while True:
        cur.execute(
            f"""
            INSERT INTO {child}
            SELECT * FROM {table}
            WHERE posted_at >= %s AND posted_at < %s
            LIMIT %s
            """,
            (start, end, batch),
        )
        moved = cur.rowcount
        if moved == 0:
            break
        cur.execute(
            f"DELETE FROM {table} WHERE posted_at >= %s AND posted_at < %s LIMIT %s",
            (start, end, batch),
        )


def migrate(year: int = 2024) -> None:
    conn = connect_db()
    conn.autocommit = True
    cur = conn.cursor()
    for category in CATEGORIES:
        ensure_year_partition(cur, category, year)
        move_rows(cur, category, year)
    cur.close()
    conn.close()


if __name__ == "__main__":  # pragma: no cover - script entry
    migrate()
