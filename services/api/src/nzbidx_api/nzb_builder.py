"""NZB builder utilities.

This module exposes :func:`build_nzb_for_release` which returns an NZB XML
document for a release using segments stored in the database. If no segments
exist the function raises :class:`newznab.NzbFetchError`.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import xml.etree.ElementTree as ET
from typing import List, Tuple

from . import config
from .db import get_connection
from .backfill_release_parts import backfill_release_parts

# Collect database exception types that should be handled.  Optional
# dependencies are imported lazily so tests can run without them installed.
DB_EXCEPTIONS: list[type[Exception]] = [sqlite3.Error]
try:  # pragma: no cover - optional dependency
    from asyncpg.exceptions import PostgresError as AsyncpgPostgresError

    DB_EXCEPTIONS.append(AsyncpgPostgresError)
except Exception:  # pragma: no cover - asyncpg not installed
    pass

try:  # pragma: no cover - optional dependency
    import psycopg

    DB_EXCEPTIONS.append(psycopg.Error)  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - psycopg not installed
    pass

DB_EXCEPTIONS = tuple(DB_EXCEPTIONS)

log = logging.getLogger(__name__)


def _segments_from_db(release_id: int | str) -> List[Tuple[int, str, str, int]]:
    from . import newznab

    conn = get_connection()
    try:
        rid = int(release_id)
        with conn.cursor() as cur:
            placeholder = (
                "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
            )
            cur.execute(
                f"SELECT segments FROM release WHERE id = {placeholder}",
                (rid,),
            )
            row = cur.fetchone()
        if not row:
            log.warning("release_not_found", extra={"release_id": rid})
            raise LookupError("release not found")
        seg_data = row[0]
        if not seg_data:
            log.warning("missing_segments", extra={"release_id": rid})
            raise LookupError("release has no segments")
        try:
            data = (
                json.loads(seg_data) if isinstance(seg_data, (str, bytes)) else seg_data
            )
        except json.JSONDecodeError as exc:
            log.warning(
                "invalid_segments_json",
                extra={"release_id": rid, "seg_data": seg_data},
            )
            raise newznab.NzbDatabaseError("invalid segment data") from exc
        segments: List[Tuple[int, str, str, int]] = []
        for seg in data or []:
            segments.append(
                (
                    int(seg.get("number", 0)),
                    str(seg.get("message_id", "")),
                    str(seg.get("group", "")),
                    int(seg.get("size", 0) or 0),
                )
            )
        if not segments:
            log.warning("missing_segments", extra={"release_id": rid})
            raise LookupError("release has no segments")
        return segments
    except LookupError:
        raise
    except DB_EXCEPTIONS as exc:
        log.warning(
            "db_query_failed",
            extra={
                "release_id": release_id,
                "exception": exc.__class__.__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise newznab.NzbDatabaseError(str(exc)) from exc


def _build_xml_from_segments(
    release_id: str, segments: List[Tuple[int, str, str, int]]
) -> str:
    root = ET.Element("nzb", xmlns=NZB_XMLNS)
    file_el = ET.SubElement(root, "file", {"subject": release_id})
    groups_el = ET.SubElement(file_el, "groups")
    for g in sorted({g for _, _, g, _ in segments}):
        if g:
            ET.SubElement(groups_el, "group").text = g
    segs_el = ET.SubElement(file_el, "segments")
    for number, msgid, _group, size in sorted(segments, key=lambda s: s[0]):
        seg_el = ET.SubElement(
            segs_el, "segment", {"bytes": str(size), "number": str(number)}
        )
        seg_el.text = msgid.strip("<>")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode()


NZB_XMLNS = "http://www.newzbin.com/DTD/2003/nzb"


def build_nzb_for_release(release_id: str) -> str:
    """Return an NZB XML document for ``release_id``.

    Segment information is retrieved from the database. When no segments are
    found a :class:`newznab.NzbFetchError` is raised.
    """

    from . import newznab

    # Ensure environment changes to timeouts are honored across calls.
    config.nntp_timeout_seconds.cache_clear()
    config.nntp_total_timeout_seconds.cache_clear()
    config.nzb_timeout_seconds.cache_clear()

    missing = config.validate_nntp_config()
    _groups = config.NNTP_GROUPS  # ensure groups are loaded
    if missing:
        raise newznab.NntpConfigError(
            f"missing NNTP configuration: {', '.join(missing)}"
        )

    rid = int(release_id)
    log.info("starting nzb build for release %s", rid)
    try:
        try:
            segments = _segments_from_db(rid)
        except LookupError as exc:
            err = str(exc).lower()
            if "has no segments" in err:
                try:
                    backfill_release_parts(release_ids=[rid])
                except ConnectionError as bf_exc:
                    log.warning(
                        "auto_backfill_connection_error",
                        extra={"release_id": rid, "error": str(bf_exc)},
                    )
                    raise newznab.NzbFetchError(
                        f"failed to fetch segments: {bf_exc}"
                    ) from bf_exc
                except Exception as bf_exc:  # pragma: no cover - unexpected
                    log.warning(
                        "auto_backfill_error",
                        extra={"release_id": rid, "error": str(bf_exc)},
                    )
                try:
                    segments = _segments_from_db(rid)
                except LookupError:
                    log.warning("auto_backfill_failed", extra={"release_id": rid})
                    raise
            else:
                raise

        if not segments:
            raise newznab.NzbFetchError("no segments found")
        max_segments = config.nzb_max_segments()
        if len(segments) > max_segments:
            log.warning(
                "segment_limit_exceeded",
                extra={
                    "release_id": rid,
                    "segment_count": len(segments),
                    "limit": max_segments,
                },
            )
            raise newznab.NzbFetchError("segment count exceeds limit")
    except LookupError as exc:
        err = str(exc).lower()
        if "not found" in err:
            msg = (
                "release not found. The backfill script may remove invalid releases; "
                "verify that the release ID is numeric."
            )
        else:
            msg = (
                "release has no segments. To populate missing segments, run scripts/"
                "backfill_release_parts.py. The backfill script may remove invalid "
                "releases; verify that the release ID is numeric."
            )
        raise newznab.NzbFetchError(msg) from exc
    except newznab.NzbFetchError:
        raise
    except newznab.NzbDatabaseError:
        raise
    except DB_EXCEPTIONS as exc:
        log.exception(
            "db_query_failed",
            extra={
                "release_id": rid,
                "exception": exc.__class__.__name__,
                "error": str(exc),
            },
        )
        raise newznab.NzbDatabaseError(str(exc)) from exc
    return _build_xml_from_segments(str(rid), segments)
