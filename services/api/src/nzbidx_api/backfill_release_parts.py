"""Backfill release_part rows for existing releases."""

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from typing import Callable, Iterable, Optional

from nzbidx_ingest.main import bulk_index_releases, connect_db, connect_opensearch
from nzbidx_api.nzb_builder import NZB_XMLNS, build_nzb_for_release
import nzbidx_api.newznab as newznab

log = logging.getLogger(__name__)

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


def backfill_release_parts(
    progress_cb: Optional[Callable[[int], None]] = None,
    release_ids: Optional[Iterable[int]] = None,
) -> int:
    """Populate ``release_part`` rows for existing releases.

    A ``progress_cb`` may be supplied to receive the number of processed
    releases after each successful iteration.  ``release_ids`` may restrict the
    job to a specific set of releases.
    """
    conn = connect_db()
    os_client = connect_opensearch()
    cur = conn.cursor()
    placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
    base_sql = "SELECT id, norm_title, source_group FROM release"
    params: list[int] | tuple[int, ...] = []
    if release_ids:
        ids = list(release_ids)
        placeholders = ",".join([placeholder] * len(ids))
        base_sql += f" WHERE id IN ({placeholders})"
        params = ids
    cur.execute(f"{base_sql} ORDER BY id", params)
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
            except Exception as exc:  # pragma: no cover - unexpected
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
            if progress_cb:
                try:
                    progress_cb(processed)
                except Exception:  # pragma: no cover - progress callback errors
                    log.exception("progress_callback_failed")
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
    return processed
