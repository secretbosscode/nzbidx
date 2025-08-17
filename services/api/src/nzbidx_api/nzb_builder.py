"""NZB builder utilities.

This module exposes :func:`build_nzb_for_release` which returns an NZB XML
document for a release using segments stored in the database. If no segments
exist the function raises :class:`newznab.NzbFetchError`.
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from typing import List, Tuple

from . import config

log = logging.getLogger(__name__)


def _segments_from_db(release_id: str) -> List[Tuple[int, str, str, int]]:
    from nzbidx_ingest.main import connect_db  # type: ignore

    conn = None
    try:
        conn = connect_db()
        cur = conn.cursor()
        placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
        cur.execute(
            f"SELECT segments FROM release WHERE norm_title = {placeholder}",
            (release_id,),
        )
        row = cur.fetchone()
        if not row:
            log.warning("release_not_found", extra={"release_id": release_id})
            raise LookupError("release not found")
        seg_data = row[0]
        if not seg_data:
            log.warning("missing_segments", extra={"release_id": release_id})
            raise LookupError("release has no segments")
        try:
            data = (
                json.loads(seg_data) if isinstance(seg_data, (str, bytes)) else seg_data
            )
        except Exception:
            data = []
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
            log.warning("missing_segments", extra={"release_id": release_id})
            raise LookupError("release has no segments")
        return segments
    except LookupError:
        raise
    except Exception as exc:
        log.warning(
            "db_query_failed",
            extra={
                "release_id": release_id,
                "exception": exc.__class__.__name__,
                "error": str(exc),
            },
        )
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
    for number, msgid, _group, size in sorted(segments, key=lambda s: s[0])[
        :MAX_SEGMENTS
    ]:
        seg_el = ET.SubElement(
            segs_el, "segment", {"bytes": str(size), "number": str(number)}
        )
        seg_el.text = msgid.strip("<>")
    return '<?xml version="1.0" encoding="utf-8"?>' + ET.tostring(
        root, encoding="unicode"
    )


NZB_XMLNS = "http://www.newzbin.com/DTD/2003/nzb"
MAX_SEGMENTS = 1000


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

    log.info("starting nzb build for release %s", release_id)
    try:
        segments = _segments_from_db(release_id)
        if not segments:
            raise newznab.NzbFetchError("no segments found")
    except LookupError as exc:
        err = str(exc).lower()
        if "not found" in err:
            msg = (
                "release not found. The backfill script may remove invalid releases; "
                "verify that the release ID is normalized."
            )
        else:
            msg = (
                "release has no segments. To populate missing segments, run scripts/"
                "backfill_release_parts.py. The backfill script may remove invalid "
                "releases; verify that the release ID is normalized."
            )
        raise newznab.NzbFetchError(msg) from exc
    except Exception as exc:
        raise newznab.NzbFetchError("database query failed") from exc
    return _build_xml_from_segments(release_id, segments)
