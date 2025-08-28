#!/usr/bin/env python
# ruff: noqa: E402
"""Prune releases outside configured size thresholds.

This helper reads the same ``MAX_RELEASE_BYTES`` environment variable used
during ingest and removes any rows where the ``size_bytes`` column falls
outside the configured range.  Additionally, it recomputes the minimum size
threshold for each release using :func:`nzbidx_ingest.config.min_size_for_release`
and prunes rows that fall below the category or title specific minimum.  All
``release`` partitions are processed to ensure every category is covered.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore
from nzbidx_ingest.config import (  # type: ignore
    CATEGORY_MIN_SIZES,
    RELEASE_MIN_EXACT,
    RELEASE_MIN_REGEX,
    min_size_for_release,
)

try:  # Prefer ingest helper but fall back to API helper if unavailable.
    from nzbidx_ingest.sql import sql_placeholder  # type: ignore
except Exception:  # pragma: no cover - fallback
    from nzbidx_api.db import sql_placeholder  # type: ignore


MAX_BYTES = int(os.getenv("MAX_RELEASE_BYTES", "0") or 0)


def prune_sizes() -> int:
    """Delete releases with ``size_bytes`` outside the configured range."""

    if (
        MAX_BYTES <= 0
        and not any(CATEGORY_MIN_SIZES.values())
        and not RELEASE_MIN_EXACT
        and not RELEASE_MIN_REGEX
    ):
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
        ids: list[int] = []
        cur.execute(f"SELECT id, norm_title, category_id, size_bytes FROM {table}")
        for rid, norm_title, category_id, size_bytes in cur.fetchall():
            cat = str(category_id) if category_id is not None else ""
            min_bytes = min_size_for_release(norm_title or "", cat)
            if min_bytes > 0 and (size_bytes or 0) < min_bytes:
                ids.append(rid)
                if len(ids) >= 1000:
                    ph = ", ".join(placeholder for _ in ids)
                    cur.execute(
                        f"DELETE FROM {table} WHERE id IN ({ph})",
                        ids,
                    )
                    total += cur.rowcount
                    ids.clear()
        if ids:
            ph = ", ".join(placeholder for _ in ids)
            cur.execute(f"DELETE FROM {table} WHERE id IN ({ph})", ids)
            total += cur.rowcount

        if MAX_BYTES > 0:
            cur.execute(
                f"DELETE FROM {table} WHERE size_bytes > {placeholder}",
                [MAX_BYTES],
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
