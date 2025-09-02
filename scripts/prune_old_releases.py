#!/usr/bin/env python3
# ruff: noqa: E402
"""Prune releases older than a configured retention period."""

from __future__ import annotations

import datetime as dt
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

# Default to 30 days when the variable is unset; "0" disables pruning.
RETENTION_DAYS = int(os.getenv("RELEASE_RETENTION_DAYS", "30") or 0)


def prune_old_releases() -> int:
    """Delete releases with ``posted_at`` older than the retention period."""
    if RETENTION_DAYS <= 0:
        return 0
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RETENTION_DAYS)
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
        cur.execute(
            f"DELETE FROM {table} WHERE posted_at < {placeholder}",
            (cutoff,),
        )
        total += cur.rowcount
    conn.commit()
    conn.close()
    return total


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    count = prune_old_releases()
    print(f"pruned {count} old releases")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
