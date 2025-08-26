#!/usr/bin/env python
# ruff: noqa: E402
"""Normalize ``release.segments`` values to the dict schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.db import sql_placeholder
from nzbidx_ingest.main import connect_db
from nzbidx_ingest.segment_schema import validate_segment_schema
from nzbidx_ingest.sql import sql_placeholder


def _convert(seg):
    if isinstance(seg, dict):
        return {
            "number": int(seg.get("number", 0)),
            "message_id": str(seg.get("message_id", "")),
            "group": str(seg.get("group", "")),
            "size": int(seg.get("size", 0) or 0),
        }
    if isinstance(seg, (list, tuple)) and len(seg) >= 4:
        n, m, g, s = seg[:4]
        return {
            "number": int(n),
            "message_id": str(m),
            "group": str(g),
            "size": int(s),
        }
    raise ValueError(f"invalid segment entry: {seg!r}")


def normalize() -> int:
    conn = connect_db()
    cur = conn.cursor()
    placeholder = sql_placeholder(conn)
    cur.execute("SELECT id, segments FROM release WHERE segments IS NOT NULL")
    rows = cur.fetchall()
    batch = []
    for rid, seg_json in rows:
        try:
            data = (
                json.loads(seg_json or "[]")
                if isinstance(seg_json, (str, bytes))
                else seg_json or []
            )
        except Exception:
            continue
        try:
            validate_segment_schema(data)
            continue
        except AssertionError:
            pass
        converted = []
        try:
            for seg in data:
                converted.append(_convert(seg))
        except Exception:
            continue
        batch.append((json.dumps(converted), rid))
    if batch:
        cur.executemany(
            f"UPDATE release SET segments = {placeholder} WHERE id = {placeholder}",
            batch,
        )
    conn.commit()
    conn.close()
    return len(batch)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    count = normalize()
    print(f"normalized {count} releases")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
