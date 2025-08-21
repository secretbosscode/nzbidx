#!/usr/bin/env python
# ruff: noqa: E402
"""Backfill segment data for existing releases.

The helper iterates over all releases, fetches NZB segment metadata via
``build_nzb_for_release`` and stores the segment details in the ``release``
table. Releases that no longer resolve to any segments are removed from storage
and pruned from the search index.

Optional release IDs may restrict the job to specific entries.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.backfill_release_parts import backfill_release_parts
from nzbidx_ingest.main import connect_db


def _auto_mode() -> None:
    """Backfill only releases missing segment rows."""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.id FROM release r
        WHERE r.has_parts AND r.segments IS NULL
        ORDER BY r.id
        """,
    )
    ids = [row[0] for row in cur.fetchall()]
    conn.close()
    if ids:
        backfill_release_parts(release_ids=ids)


def main(
    argv: list[str] | None = None,
) -> None:  # pragma: no cover - integration script
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--auto",
        action="store_true",
        help="process only releases marked with has_parts but missing segments",
    )
    parser.add_argument(
        "release_ids",
        nargs="*",
        type=int,
        metavar="ID",
        help="specific release IDs to backfill",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.auto:
        _auto_mode()
    elif args.release_ids:
        backfill_release_parts(release_ids=args.release_ids)
    else:
        backfill_release_parts()


if __name__ == "__main__":
    main()

