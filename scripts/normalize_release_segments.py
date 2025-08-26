#!/usr/bin/env python
# ruff: noqa: E402
"""Normalize ``release.segments`` values to the dict schema."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db
from nzbidx_ingest.segment_schema import validate_segment_schema
from nzbidx_api.json_utils import orjson


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
    placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
    cur.execute("SELECT id, segments FROM release WHERE segments IS NOT NULL")
    rows = cur.fetchall()
    updated = 0
    for rid, seg_json in rows:
        try:
            data = (
                orjson.loads(seg_json or "[]")
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
        cur.execute(
            f"UPDATE release SET segments = {placeholder} WHERE id = {placeholder}",
            (orjson.dumps(converted).decode(), rid),
        )
        updated += 1
    conn.commit()
    conn.close()
    return updated


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI helper
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    count = normalize()
    print(f"normalized {count} releases")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
