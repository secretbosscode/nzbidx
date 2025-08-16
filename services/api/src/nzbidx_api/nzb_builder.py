"""Thin NZB builder interface.

This module exposes :func:`build_nzb_for_release` which connects to an NNTP
server to build a real NZB document.  Missing configuration or empty results
raise :class:`newznab.NzbFetchError` while unexpected failures fall back to a
stub NZB document for compatibility.  The overall runtime is capped by the
``NNTP_TOTAL_TIMEOUT`` environment variable.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from importlib import util
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

from . import config


def _segments_from_db(release_id: str) -> List[Tuple[int, str, str, int]]:
    try:
        from nzbidx_ingest.main import connect_db  # type: ignore
    except Exception:
        return []
    try:
        conn = connect_db()
    except Exception:
        return []
    try:
        cur = conn.cursor()
        placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
        cur.execute(
            f"SELECT id FROM release WHERE norm_title = {placeholder}", (release_id,)
        )
        row = cur.fetchone()
        if not row:
            return []
        rid = row[0]
        cur.execute(
            f"SELECT segment_number, message_id, group_name, size_bytes FROM release_part WHERE release_id = {placeholder} ORDER BY segment_number",
            (rid,),
        )
        return [(int(a), str(b), str(c), int(d or 0)) for a, b, c, d in cur.fetchall()]
    except Exception:
        return []
    finally:
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
    for number, msgid, group, size in sorted(segments, key=lambda s: s[0])[
        :MAX_SEGMENTS
    ]:
        seg_el = ET.SubElement(
            segs_el, "segment", {"bytes": str(size), "number": str(number)}
        )
        seg_el.text = msgid.strip("<>")
    return '<?xml version="1.0" encoding="utf-8"?>' + ET.tostring(
        root, encoding="unicode"
    )


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

NNTPPermanentError = getattr(
    nntplib, "NNTPPermanentError", type("NNTPPermanentError", (Exception,), {})
)
NNTPTemporaryError = getattr(
    nntplib, "NNTPTemporaryError", type("NNTPTemporaryError", (Exception,), {})
)

log = logging.getLogger(__name__)


_group_list_cache: dict[str, List[str]] = {}
# Cache for autodiscovered groups when NNTP_GROUPS is unset.  The full list
# is cached so per-request limits can be applied without re-listing groups.
_discovered_groups: List[str] | None = None


def build_nzb_for_release(release_id: str) -> str:
    """Return an NZB XML document for ``release_id``.

    The function connects to the NNTP server configured via environment
    variables (``NNTP_HOST``, ``NNTP_PORT``, ``NNTP_USER``, ``NNTP_PASS`` and
    ``NNTP_GROUPS``).  All articles whose subject contains ``release_id`` are
    collected and stitched into NZB ``<file>`` and ``<segment>`` elements.  The
    NNTP ``XOVER`` range is capped to the most recent ``NNTP_XOVER_LIMIT``
    articles (default ``1000``) to avoid fetching unbounded history.  Total
    runtime across retries is bounded by ``NNTP_TOTAL_TIMEOUT`` (default
    ``60`` seconds).

    When mandatory configuration is missing or no matching articles are found
    an :class:`newznab.NzbFetchError` is raised.  Other unexpected errors are
    logged and a minimal stub NZB is returned for compatibility.
    """

    from . import newznab

    # Ensure environment changes to timeouts are honored across calls.
    config.nntp_timeout_seconds.cache_clear()
    config.nntp_total_timeout_seconds.cache_clear()
    config.nzb_timeout_seconds.cache_clear()
    segments = _segments_from_db(release_id)
    if segments:
        return _build_xml_from_segments(release_id, segments)

    host = os.getenv("NNTP_HOST")
    if not host:
        raise newznab.NzbFetchError(
            "NNTP_HOST not configured; set the NNTP_HOST environment variable"
        )

    port = int(os.getenv("NNTP_PORT", "119"))
    user = os.getenv("NNTP_USER")
    password = os.getenv("NNTP_PASS")
    ssl_env = os.getenv("NNTP_SSL")
    use_ssl = (ssl_env == "1") if ssl_env is not None else port == nntplib.NNTP_SSL_PORT
    conn_cls = nntplib.NNTP_SSL if use_ssl else nntplib.NNTP

    group_env = os.getenv("NNTP_GROUPS", "")
    try:
        group_limit = int(os.getenv("NNTP_GROUP_LIMIT", "0"))
    except ValueError:
        group_limit = 0

    entries: list[str] = []
    if group_env.strip():
        entries = [g.strip() for g in group_env.split(",") if g.strip()]
        static_groups = [g for g in entries if "*" not in g and "?" not in g]
        patterns = [g for g in entries if "*" in g or "?" in g]
    else:
        patterns = []
        groups = _group_list_cache.get("__all__")
        if groups is None:
            try:
                from nzbidx_ingest.config import _load_groups as ingest_load_groups

                groups = ingest_load_groups()
            except Exception:
                groups = []
            _group_list_cache["__all__"] = groups
        static_groups = list(groups)
        if group_limit and len(static_groups) > group_limit:
            static_groups = static_groups[:group_limit]

    start = time.monotonic()
    max_secs = config.nntp_total_timeout_seconds()

    try:
        delay = 1
        for attempt in range(1, 4):
            if time.monotonic() - start > max_secs:
                raise newznab.NzbFetchError("nntp timeout exceeded")
            try:
                log.info("NNTP connection attempt %d", attempt)
                with conn_cls(
                    host,
                    port,
                    user=user,
                    password=password,
                    readermode=True,
                    timeout=float(config.nntp_timeout_seconds()),
                ) as server:
                    groups = list(static_groups)
                    if not entries:
                        global _discovered_groups
                        if _discovered_groups is None:
                            try:
                                _resp, listing = server.list()
                                _discovered_groups = [
                                    (
                                        g[0]
                                        if isinstance(g, (tuple, list))
                                        else str(g).split()[0]
                                    )
                                    for g in listing
                                ]
                            except Exception:
                                _discovered_groups = []
                        groups.extend(_discovered_groups)
                    elif patterns and not (group_limit and len(groups) >= group_limit):
                        for pattern in patterns:
                            cached = _group_list_cache.get(pattern)
                            if cached is None:
                                try:
                                    _resp, listing = server.list(pattern)
                                    cached = [
                                        (
                                            g[0]
                                            if isinstance(g, (tuple, list))
                                            else str(g).split()[0]
                                        )
                                        for g in listing
                                    ]
                                except Exception:
                                    cached = []
                                _group_list_cache[pattern] = cached
                            groups.extend(cached)
                            if group_limit and len(groups) >= group_limit:
                                groups = groups[:group_limit]
                                break
                    if group_limit and len(groups) > group_limit:
                        groups = groups[:group_limit]
                    if not groups:
                        raise newznab.NzbFetchError(
                            "no NNTP groups configured; check NNTP_GROUPS"
                        )
                    files: Dict[str, List[Tuple[int, int, str]]] = {}
                    for group in groups:
                        try:
                            _resp, _count, first, last, _name = server.group(group)
                            limit = int(os.getenv("NNTP_XOVER_LIMIT", "1000"))
                            first_num, last_num = int(first), int(last)
                            xover_start = max(last_num - limit + 1, first_num)
                            _resp, overviews = server.xover(xover_start, last_num)
                        except Exception:
                            continue
                        for ov in overviews:
                            fields = ov[1] if isinstance(ov, (tuple, list)) else ov
                            subject = str(fields.get("subject", ""))
                            if release_id not in subject:
                                continue
                            message_id = str(fields.get("message-id") or "").strip()
                            if not message_id:
                                log.debug(
                                    "skipping overview without message-id: %s", fields
                                )
                                continue
                            seg_num = _extract_segment_number(subject)
                            filename = _extract_filename(subject) or release_id
                            size = int(fields.get("bytes") or 0)
                            if size == 0:
                                try:
                                    _resp, _num, _mid, lines = server.body(
                                        message_id, decode=False
                                    )
                                    size = sum(len(line) for line in lines)
                                except Exception:
                                    pass
                            files.setdefault(filename, []).append(
                                (seg_num, size, message_id)
                            )
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
                            seg_el.text = msgid.strip("<>")
                    return '<?xml version="1.0" encoding="utf-8"?>' + ET.tostring(
                        root, encoding="unicode"
                    )
            except newznab.NzbFetchError:
                raise
            except nntplib.NNTPPermanentError as exc:
                log.error("NNTP permanent error on attempt %d: %s", attempt, exc)
                raise newznab.NzbFetchError(str(exc)) from exc
            except Exception as exc:
                if attempt == 3:
                    raise
                log.warning(
                    "NNTP connection attempt %d failed: %s; retrying in %s seconds",
                    attempt,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
    except newznab.NzbFetchError:
        raise
    except (NNTPPermanentError, NNTPTemporaryError, OSError) as exc:
        log.warning("nntp connection failed for %s: %s", release_id, exc)
        raise newznab.NzbFetchError("nntp connection failed") from exc
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
