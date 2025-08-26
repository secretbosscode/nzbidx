#!/usr/bin/env python
"""Validate stored release categories against heuristic inference."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import _infer_category, connect_db  # noqa: E402

BATCH_SIZE = 1000


def main(
    argv: list[str] | None = None,
) -> None:  # pragma: no cover - integration script
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rewrite",
        action="store_true",
        help="rewrite incorrect category_id values",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT norm_title, category_id, tags, source_group FROM release")
    mismatches: list[tuple[str, int | None, str]] = []
    total = 0

    while True:
        batch = cur.fetchmany(BATCH_SIZE)
        if not batch:
            break
        for norm_title, category_id, tags, source_group in batch:
            total += 1
            title = norm_title or ""
            tag_items = " ".join(f"[{t}]" for t in (tags or "").split(",") if t)
            pseudo_subject = f"{title} {tag_items}".strip()
            expected = _infer_category(pseudo_subject, str(source_group))
            if expected is None:
                continue
            expected_int = int(expected)
            stored_int = int(category_id) if category_id is not None else None
            if stored_int != expected_int:
                mismatches.append((title, stored_int, expected))

    logging.info("Checked %d releases, %d mismatches", total, len(mismatches))
    for title, stored, expected in mismatches[:20]:
        logging.info("%r stored=%s expected=%s", title, stored, expected)

    if args.rewrite and mismatches:
        confirm = input(f"Rewrite {len(mismatches)} category_id values? [y/N]: ")
        if confirm.lower().startswith("y"):
            with conn:
                for title, stored, expected in mismatches:
                    if stored is None:
                        cur.execute(
                            "UPDATE release SET category_id = ? WHERE norm_title = ? AND category_id IS NULL",
                            (int(expected), title),
                        )
                    else:
                        cur.execute(
                            "UPDATE release SET category_id = ? WHERE norm_title = ? AND category_id = ?",
                            (int(expected), title, stored),
                        )
            logging.info("Rewrote %d rows", len(mismatches))
        else:
            logging.info("Aborted rewrite")
    conn.close()


if __name__ == "__main__":
    main()
