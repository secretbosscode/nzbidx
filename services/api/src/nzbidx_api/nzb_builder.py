"""Thin NZB builder interface.

This module exposes :func:`build_nzb_for_release` which connects to an NNTP
server to build a real NZB document.  Missing configuration or empty results
raise :class:`newznab.NzbFetchError` while unexpected failures fall back to a
stub NZB document for compatibility.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from importlib import util
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

# Prefer the bundled ``nntplib`` implementation over the deprecated
# standard library version.  Insert the local ``services/api/src`` path at
# the start of ``sys.path`` so resolving the module picks up our copy.
_local_path = Path(__file__).resolve().parents[1]
if str(_local_path) not in sys.path:
    sys.path.insert(0, str(_local_path))

_nntplib_file = _local_path / "nntplib.py"
spec = util.spec_from_file_location("nntplib_local", _nntplib_file)
assert spec and spec.loader  # narrow types for mypy-like checks
nntplib = util.module_from_spec(spec)
spec.loader.exec_module(nntplib)


NZB_XMLNS = "http://www.newzbin.com/DTD/2003/nzb"
MAX_SEGMENTS = 1000

log = logging.getLogger(__name__)


def build_nzb_for_release(release_id: str) -> str:
    """Return an NZB XML document for ``release_id``.

    The function connects to the NNTP server configured via environment
    variables (``NNTP_HOST``, ``NNTP_PORT``, ``NNTP_USER``, ``NNTP_PASS`` and
    ``NNTP_GROUPS``).  All articles whose subject contains ``release_id`` are
    collected and stitched into NZB ``<file>`` and ``<segment>`` elements.  The
    NNTP ``XOVER`` range is capped to the most recent ``NNTP_XOVER_LIMIT``
    articles (default ``1000``) to avoid fetching unbounded history.

    When mandatory configuration is missing or no matching articles are found
    an :class:`newznab.NzbFetchError` is raised.  Other unexpected errors are
    logged and a minimal stub NZB is returned for compatibility.
    """

    from . import newznab

    host = os.getenv("NNTP_HOST")
    if not host:
        raise newznab.NzbFetchError("NNTP_HOST not configured")

    port = int(os.getenv("NNTP_PORT", "119"))
    user = os.getenv("NNTP_USER")
    password = os.getenv("NNTP_PASS")
    ssl_env = os.getenv("NNTP_SSL")
    use_ssl = (ssl_env == "1") if ssl_env is not None else port == nntplib.NNTP_SSL_PORT
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
                raise newznab.NzbFetchError("no NNTP groups configured")
            files: Dict[str, List[Tuple[int, int, str]]] = {}
            for group in groups:
                try:
                    _resp, _count, first, last, _name = server.group(group)
                    limit = int(os.getenv("NNTP_XOVER_LIMIT", "1000"))
                    first_num, last_num = int(first), int(last)
                    start = max(last_num - limit + 1, first_num)
                    _resp, overviews = server.xover(start, last_num)
                except Exception:
                    continue
                for ov in overviews:
                    subject = str(ov.get("subject", ""))
                    if release_id not in subject:
                        continue
                    message_id = str(ov.get("message-id", ""))
                    seg_num = _extract_segment_number(subject)
                    filename = _extract_filename(subject) or release_id
                    size = int(ov.get("bytes") or 0)
                    if size == 0 and message_id:
                        try:
                            _resp, _num, _mid, lines = server.body(
                                message_id, decode=False
                            )
                            size = sum(len(line) for line in lines)
                        except Exception:
                            pass
                    files.setdefault(filename, []).append((seg_num, size, message_id))
            if not files:
                raise newznab.NzbFetchError("no matching articles found")

            root = ET.Element("nzb", xmlns=NZB_XMLNS)
            for filename, segments in files.items():
                file_el = ET.SubElement(root, "file", {"subject": filename})
                groups_el = ET.SubElement(file_el, "groups")
                for g in groups:
                    ET.SubElement(groups_el, "group").text = g
                segments_el = ET.SubElement(file_el, "segments")
                for number, size, msgid in sorted(segments, key=lambda s: s[0])[
                    :MAX_SEGMENTS
                ]:
                    seg_el = ET.SubElement(
                        segments_el,
                        "segment",
                        {"bytes": str(size), "number": str(number)},
                    )
                    seg_el.text = msgid
            return '<?xml version="1.0" encoding="utf-8"?>' + ET.tostring(
                root, encoding="unicode"
            )
    except newznab.NzbFetchError:
        raise
    except Exception as exc:
        log.exception("nzb build failed for %s: %s", release_id, exc)
        return newznab.nzb_xml_stub(release_id)


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
