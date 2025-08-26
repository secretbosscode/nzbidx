#!/usr/bin/env python
# ruff: noqa: E402
"""Inspect segments for a release.

Given a numeric release ID this helper fetches the ``segments`` column from
``release`` and prints the segment count along with the first and last
message-ids.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.db import get_connection, sql_placeholder


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_id", type=int, help="Numeric release ID to inspect")
    args = parser.parse_args(argv)

    conn = get_connection()
    with conn.cursor() as cur:
        placeholder = sql_placeholder(conn)
        cur.execute(
            f"SELECT segments FROM release WHERE id = {placeholder}",
            (args.release_id,),
        )
        row = cur.fetchone()

    if not row or not row[0]:
        print("no segments found")
        return

    seg_data = row[0]
    segments = (
        json.loads(seg_data) if isinstance(seg_data, (str, bytes)) else seg_data
    ) or []

    if not segments:
        print("no segments found")
        return

    segments.sort(key=lambda s: int(s.get("number", 0)))
    count = len(segments)
    first_id = segments[0].get("message_id", "")
    last_id = segments[-1].get("message_id", "")
    print(f"segments: {count}")
    print(f"first message-id: {first_id}")
    print(f"last message-id: {last_id}")


if __name__ == "__main__":  # pragma: no cover - CLI helper
    main()
