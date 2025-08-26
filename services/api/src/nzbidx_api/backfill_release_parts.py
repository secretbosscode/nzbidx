"""Backfill segment metadata for existing releases."""

from __future__ import annotations

import json
import logging
import os
from contextlib import closing
from typing import Callable, Iterable, Optional

from nzbidx_ingest.main import connect_db
from nzbidx_ingest.nntp_client import NNTPClient
from nzbidx_ingest.parsers import extract_segment_number, normalize_subject
from nzbidx_ingest.segment_schema import validate_segment_schema
from . import config

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "100"))
XOVER_LOOKBACK = int(os.getenv("BACKFILL_XOVER_LOOKBACK", "10000"))


def _fetch_segments(release_id: str, group: str) -> list[tuple[int, str, int]]:
    """Return ``(number, message_id, size)`` tuples for ``release_id``."""
    client = NNTPClient()
    groups = [group] if group else config.NNTP_GROUPS
    last_exc: Exception | None = None
    last_group = ""
    for grp in groups:
        try:
            high = client.high_water_mark(grp)
            start = max(0, high - XOVER_LOOKBACK + 1) if XOVER_LOOKBACK > 0 else 0
            headers = client.xover(grp, start, high) if high > 0 else []
        except Exception as exc:
            last_exc = exc
            last_group = grp
            continue
        segments: list[tuple[int, str, int]] = []
        target = release_id.lower()
        seen_numbers: set[int] = set()
        for header in headers:
            subject = str(header.get("subject", ""))
            if normalize_subject(subject) != target:
                continue
            msg_id = str(header.get("message-id") or "").strip("<>")
            if not msg_id:
                continue
            size = int(header.get("bytes") or 0)
            if size <= 0:
                size = client.body_size(msg_id)
            if size <= 0:
                continue
            number = extract_segment_number(subject)
            if number in seen_numbers:
                continue
            segments.append((number, msg_id, size))
            seen_numbers.add(number)
            if (
                1 in seen_numbers
                and len(seen_numbers) == max(seen_numbers)
                and max(seen_numbers) > 1
            ):
                break
        if segments:
            segments.sort(key=lambda s: s[0])
            return segments
    if last_exc:
        raise ConnectionError(
            f"error fetching segments for {release_id} in {last_group}: {last_exc}"
        ) from last_exc
    return []


def backfill_release_parts(
    progress_cb: Optional[Callable[[int], None]] = None,
    release_ids: Optional[Iterable[int]] = None,
    auto: bool = False,
) -> int:
    """Populate segment metadata for existing releases.

    A ``progress_cb`` may be supplied to receive the number of processed
    releases after each successful iteration. ``release_ids`` may restrict the
    job to a specific set of releases.  When ``auto`` is ``True``, only releases
    marked with ``has_parts`` but missing ``segments`` are processed.
    """
    if not config.NNTP_GROUPS:
        config.validate_nntp_config()
    conn = connect_db()
    try:
        _cursor = conn.cursor()
        cursor_cm = _cursor if hasattr(_cursor, "__enter__") else closing(_cursor)
        with cursor_cm as cur:
            placeholder = (
                "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
            )
            base_sql = "SELECT id, norm_title, source_group, segments FROM release"
            params: list[int] | tuple[int, ...] = []
            if auto and not release_ids:
                cur.execute(
                    """
                    SELECT id FROM release
                    WHERE has_parts AND segments IS NULL
                    ORDER BY id
                    """,
                )
                release_ids = [row[0] for row in cur.fetchall()]
                if not release_ids:
                    return 0
            if release_ids:
                ids = list(release_ids)
                placeholders = ",".join([placeholder] * len(ids))
                base_sql += f" WHERE id IN ({placeholders})"
                params = ids
            cur.execute(f"{base_sql} ORDER BY id", params)
            processed = 0
            to_delete: list[tuple[int, str]] = []
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for rel_id, norm_title, group, existing in rows:
                    if existing:
                        log.info("segments_exist", extra={"id": rel_id})
                        continue
                    try:
                        segments = _fetch_segments(norm_title, group or "")
                    except ConnectionError as exc:
                        log.warning(
                            "nntp_fetch_failed",
                            extra={"id": rel_id, "group": group, "error": str(exc)},
                        )
                        raise
                    except Exception as exc:  # pragma: no cover - unexpected
                        log.warning(
                            "unexpected_error", extra={"id": rel_id, "error": str(exc)}
                        )
                        to_delete.append((rel_id, norm_title))
                        continue
                    if not segments:
                        log.info("no_segments", extra={"id": rel_id})
                        to_delete.append((rel_id, norm_title))
                        continue
                    seg_data = [
                        {
                            "number": num,
                            "message_id": msg_id,
                            "group": group or "",
                            "size": size,
                        }
                        for num, msg_id, size in segments
                    ]
                    validate_segment_schema(seg_data)
                    total_size = sum(size for _, _, size in segments)
                    conn.execute(
                        (
                            f"UPDATE release SET segments = {placeholder}, has_parts = {placeholder}, "
                            f"part_count = {placeholder}, size_bytes = {placeholder} WHERE id = {placeholder}"
                        ),
                        (json.dumps(seg_data), True, len(seg_data), total_size, rel_id),
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
                    placeholders = ",".join([placeholder] * len(ids))
                    conn.execute(
                        f"DELETE FROM release WHERE id IN ({placeholders})", ids
                    )
                    conn.commit()
                    log.info("deleted %d invalid releases", len(ids))
                    to_delete.clear()
                log.info("processed %d releases", processed)
            return processed
    finally:
        conn.close()
