"""Thin NZB builder interface.

This module exposes :func:`build_nzb_for_release` which connects to an NNTP
server to build a real NZB document.  If the NNTP connection fails or no
articles are found a small stub document is returned instead.
"""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import nntplib


NZB_XMLNS = "http://www.newzbin.com/DTD/2003/nzb"


def build_nzb_for_release(release_id: str) -> str:
    """Return an NZB XML document for ``release_id``.

    The function connects to the NNTP server configured via environment
    variables (``NNTP_HOST``, ``NNTP_PORT``, ``NNTP_USER``, ``NNTP_PASS`` and
    ``NNTP_GROUPS``).  All articles whose subject contains ``release_id`` are
    collected and stitched into NZB ``<file>`` and ``<segment>`` elements.

    When no NNTP configuration is present or the fetch fails a minimal stub
    NZB is returned for compatibility.
    """

    host = os.getenv("NNTP_HOST")
    if not host:
        from .newznab import nzb_xml_stub

        return nzb_xml_stub(release_id)

    port = int(os.getenv("NNTP_PORT", "119"))
    user = os.getenv("NNTP_USER")
    password = os.getenv("NNTP_PASS")
    use_ssl = os.getenv("NNTP_SSL") == "1"
    conn_cls = nntplib.NNTP_SSL if use_ssl else nntplib.NNTP

    try:
        with conn_cls(
            host,
            port,
            user=user,
            password=password,
            readermode=True,
            timeout=10,
        ) as server:
            groups = [
                g.strip() for g in os.getenv("NNTP_GROUPS", "").split(",") if g.strip()
            ]
            if not groups:
                try:
                    _resp, listing = server.list()
                    groups = [
                        g[0] if isinstance(g, (tuple, list)) else str(g).split()[0]
                        for g in listing
                    ]
                except Exception:
                    groups = []
            if not groups:
                from .newznab import nzb_xml_stub

                return nzb_xml_stub(release_id)
            files: Dict[str, List[Tuple[int, int, str]]] = {}
            for group in groups:
                try:
                    _resp, _count, first, last, _name = server.group(group)
                    _resp, overviews = server.xover(first, last)
                except Exception:
                    continue
                for ov in overviews:
                    subject = str(ov.get("subject", ""))
                    if release_id not in subject:
                        continue
                    message_id = str(ov.get("message-id", ""))
                    seg_num = _extract_segment_number(subject)
                    filename = _extract_filename(subject) or release_id
                    size = 0
                    if message_id:
                        try:
                            _resp, _num, _mid, lines = server.body(
                                message_id, decode=False
                            )
                            size = sum(len(line) for line in lines)
                        except Exception:
                            size = int(ov.get("bytes") or 0)
                    files.setdefault(filename, []).append((seg_num, size, message_id))
            if not files:
                from .newznab import nzb_xml_stub

                return nzb_xml_stub(release_id)

            root = ET.Element("nzb", xmlns=NZB_XMLNS)
            for filename, segments in files.items():
                file_el = ET.SubElement(root, "file", {"subject": filename})
                groups_el = ET.SubElement(file_el, "groups")
                for g in groups:
                    ET.SubElement(groups_el, "group").text = g
                segments_el = ET.SubElement(file_el, "segments")
                for number, size, msgid in sorted(segments, key=lambda s: s[0]):
                    seg_el = ET.SubElement(
                        segments_el,
                        "segment",
                        {"bytes": str(size), "number": str(number)},
                    )
                    seg_el.text = msgid
            return '<?xml version="1.0" encoding="utf-8"?>' + ET.tostring(
                root, encoding="unicode"
            )
    except Exception:
        from .newznab import nzb_xml_stub

        return nzb_xml_stub(release_id)


def _extract_filename(subject: str) -> str | None:
    """Return filename from a ``subject`` if present."""

    match = re.search(r'"(.+?)"', subject)
    return match.group(1) if match else None


def _extract_segment_number(subject: str) -> int:
    """Return the segment number parsed from ``subject`` if possible."""

    match = re.search(r"\((\d+)/", subject)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return 1
