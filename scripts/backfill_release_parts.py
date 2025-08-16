#!/usr/bin/env python
# ruff: noqa: E402
"""Backfill release_part rows from existing releases.

The helper iterates over all releases, fetches NZB segment metadata via
``build_nzb_for_release`` and stores the individual segment details in the
``release_part`` table. Releases that no longer resolve to any segments are
removed from storage and pruned from the search index.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.backfill_release_parts import backfill_release_parts


def main() -> None:  # pragma: no cover - integration script
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    backfill_release_parts()


if __name__ == "__main__":
    main()
