#!/usr/bin/env python
# ruff: noqa: E402
"""Prune releases outside configured size thresholds.

This helper removes rows whose ``size_bytes`` fall outside the range
configured for their category. The maximum allowed size is still controlled by
the ``MAX_RELEASE_BYTES`` environment variable, but the minimum threshold is
determined per category using :func:`nzbidx_ingest.config.min_size_for_release`.
It iterates over the ``release`` table and any partitioned tables to ensure all
categories are covered.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore
from nzbidx_ingest.config import min_size_for_release  # type: ignore

try:  # Prefer ingest helper but fall back to API helper if unavailable.
    from nzbidx_ingest.sql import sql_placeholder  # type: ignore
except Exception:  # pragma: no cover - fallback
    from nzbidx_api.db import sql_placeholder  # type: ignore

MAX_BYTES = int(os.getenv("MAX_RELEASE_BYTES", "0") or 0)


def prune_sizes() -> int:
    """Delete releases with ``size_bytes`` outside the configured range."""

    conn = connect_db()
    cur = conn.cursor()
    placeholder = sql_placeholder(conn)

    tables = ["release"]
    try:
        cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'release_%'")
        tables.extend(row[0] for row in cur.fetchall())
    except Exception:  # pragma: no cover - best effort
        pass

    total = 0
    for table in tables:
        # Prune releases smaller than the category's configured minimum size.
        cur.execute(f"SELECT DISTINCT category_id FROM {table}")
        categories = [row[0] for row in cur.fetchall() if row[0] is not None]
        for category_id in categories:
            min_bytes = min_size_for_release("", str(category_id))
            if min_bytes <= 0:
                continue
            cur.execute(
                f"DELETE FROM {table} WHERE category_id = {placeholder} AND size_bytes < {placeholder}",
                (category_id, min_bytes),
            )
            total += cur.rowcount

        # Prune releases larger than the globally configured maximum.
        if MAX_BYTES > 0:
            cur.execute(
                f"DELETE FROM {table} WHERE size_bytes > {placeholder}",
                (MAX_BYTES,),
            )
            total += cur.rowcount

    conn.commit()
    conn.close()
    return total


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    count = prune_sizes()
    print(f"pruned {count} releases")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
