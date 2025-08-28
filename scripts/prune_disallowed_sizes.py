#!/usr/bin/env python
# ruff: noqa: E402
"""Prune releases outside configured size thresholds.

This helper reads the same ``MIN_RELEASE_BYTES`` and ``MAX_RELEASE_BYTES``
environment variables used during ingest and removes any rows where the
``size_bytes`` column falls outside the configured range. It iterates over the
``release`` table and any partitioned tables to ensure all categories are
covered.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore

try:  # Prefer ingest helper but fall back to API helper if unavailable.
    from nzbidx_ingest.sql import sql_placeholder  # type: ignore
except Exception:  # pragma: no cover - fallback
    from nzbidx_api.db import sql_placeholder  # type: ignore


MIN_BYTES = int(os.getenv("MIN_RELEASE_BYTES", "0") or 0)
MAX_BYTES = int(os.getenv("MAX_RELEASE_BYTES", "0") or 0)


def prune_sizes() -> int:
    """Delete releases with ``size_bytes`` outside the configured range."""

    if MIN_BYTES <= 0 and MAX_BYTES <= 0:
        logging.warning("No size thresholds configured; skipping pruning.")
        return 0

    conn = connect_db()
    cur = conn.cursor()
    placeholder = sql_placeholder(conn)

    tables = ["release"]
    try:
        cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'release_%'")
        tables.extend(row[0] for row in cur.fetchall())
    except Exception:
        pass

    total = 0
    for table in tables:
        conditions: list[str] = []
        params: list[int] = []
        if MIN_BYTES > 0:
            conditions.append(f"size_bytes < {placeholder}")
            params.append(MIN_BYTES)
        if MAX_BYTES > 0:
            conditions.append(f"size_bytes > {placeholder}")
            params.append(MAX_BYTES)
        if not conditions:
            continue
        where = " OR ".join(conditions)
        cur.execute(f"DELETE FROM {table} WHERE {where}", params)
        total += cur.rowcount
    conn.commit()
    conn.close()
    return total


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    count = prune_sizes()
    print(f"pruned {count} releases")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
