#!/usr/bin/env python
# ruff: noqa: E402
"""Reindex all releases into OpenSearch."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import (  # type: ignore
    bulk_index_releases,
    connect_db,
    connect_opensearch,
)

BATCH_SIZE = 100


def reindex_all(
    *,
    conn: Any | None = None,
    os_client: object | None = None,
    batch_size: int = BATCH_SIZE,
) -> None:
    """Stream all releases with parts to OpenSearch."""
    if conn is None:
        conn = connect_db()
    if os_client is None:
        os_client = connect_opensearch()

    cur = conn.cursor()
    cur.execute(
        """
        SELECT norm_title, category, language, tags, source_group,
               size_bytes, posted_at, has_parts, part_count
        FROM release
        WHERE has_parts
        ORDER BY id
        """
    )
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        docs: list[tuple[str, dict[str, object]]] = []
        for (
            norm_title,
            category,
            language,
            tags,
            source_group,
            size_bytes,
            posted_at,
            has_parts,
            part_count,
        ) in rows:
            body: dict[str, object] = {
                "norm_title": norm_title,
                "has_parts": bool(has_parts),
                "part_count": int(part_count or 0),
            }
            if category:
                body["category"] = category
            if language:
                body["language"] = language
            if tags:
                body["tags"] = [t for t in str(tags).split(",") if t]
            if source_group:
                body["source_group"] = source_group
            if size_bytes:
                body["size_bytes"] = int(size_bytes)
            if posted_at:
                body["posted_at"] = posted_at
            docs.append((norm_title, body))
        bulk_index_releases(os_client, docs)
    conn.close()


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - script
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE, help="rows fetched per batch"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    reindex_all(batch_size=args.batch_size)


if __name__ == "__main__":  # pragma: no cover - script
    main()
