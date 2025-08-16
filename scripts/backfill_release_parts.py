#!/usr/bin/env python
# ruff: noqa: E402
"""Backfill release_part rows from existing releases.

The helper iterates over all releases, fetches NZB segment metadata via
``build_nzb_for_release`` and stores the individual segment details in the
``release_part`` table.  Releases that no longer resolve to any segments are
removed from storage and pruned from the search index.
"""

from __future__ import annotations

import logging
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import (  # type: ignore
    bulk_index_releases,
    connect_db,
    connect_opensearch,
)
from nzbidx_api.nzb_builder import NZB_XMLNS, build_nzb_for_release  # type: ignore
import nzbidx_api.newznab as newznab  # type: ignore

log = logging.getLogger("backfill_release_parts")

BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "100"))


def _fetch_segments(release_id: str) -> list[tuple[int, str, int]]:
    """Return ``(number, message_id, size)`` tuples for ``release_id``."""
    xml = build_nzb_for_release(release_id)
    root = ET.fromstring(xml)
    segments: list[tuple[int, str, int]] = []
    for seg in root.findall(f".//{{{NZB_XMLNS}}}segment"):
        msg_id = (seg.text or "").strip()
        size = int(seg.attrib.get("bytes", "0"))
        number = int(seg.attrib.get("number", "0"))
        segments.append((number, msg_id, size))
    return segments


def main() -> None:  # pragma: no cover - integration script
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    conn = connect_db()
    os_client = connect_opensearch()
    cur = conn.cursor()
    cur.execute("SELECT id, norm_title, source_group FROM release ORDER BY id")
    placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
    insert_sql = (
        "INSERT INTO release_part (release_id, number, message_id, source_group, size_bytes) "
        f"VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})"
    )
    processed = 0
    to_delete: list[tuple[int, str]] = []
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for rel_id, norm_title, group in rows:
            try:
                segments = _fetch_segments(norm_title)
            except newznab.NzbFetchError:
                log.warning("nntp_fetch_failed", extra={"id": rel_id})
                to_delete.append((rel_id, norm_title))
                continue
            except Exception as exc:
                log.warning("unexpected_error", extra={"id": rel_id, "error": str(exc)})
                to_delete.append((rel_id, norm_title))
                continue
            if not segments:
                log.info("no_segments", extra={"id": rel_id})
                to_delete.append((rel_id, norm_title))
                continue
            conn.executemany(
                insert_sql,
                [
                    (rel_id, num, msg_id, group or "", size)
                    for num, msg_id, size in segments
                ],
            )
            processed += 1
        conn.commit()
        if to_delete:
            ids = [r for r, _ in to_delete]
            titles = [t for _, t in to_delete]
            placeholders = ",".join([placeholder] * len(ids))
            conn.execute(f"DELETE FROM release WHERE id IN ({placeholders})", ids)
            conn.commit()
            bulk_index_releases(os_client, [(t, None) for t in titles])
            log.info("deleted %d invalid releases", len(ids))
            to_delete.clear()
        log.info("processed %d releases", processed)
    conn.close()


if __name__ == "__main__":
    main()
