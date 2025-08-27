#!/usr/bin/env python3
# ruff: noqa: E402
"""Delete releases with extensions outside the allowed sets."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import prune_disallowed_filetypes as _prune


def main() -> None:  # pragma: no cover - CLI helper
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    deleted = _prune()
    logging.info(
        "prune_disallowed_filetypes_complete",
        extra={"deleted": deleted},
    )


if __name__ == "__main__":
    main()
