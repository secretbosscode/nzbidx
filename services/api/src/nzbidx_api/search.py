"""OpenSearch query helpers."""

from __future__ import annotations

from typing import Any, Dict, List

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import quote

from .config import _int_env, search_timeout_ms
from .middleware_circuit import CircuitOpenError, call_with_retry, os_breaker
from .otel import start_span
from nzbidx_common.os import OS_RELEASES_ALIAS

try:  # pragma: no cover - optional dependency
    from opensearchpy import OpenSearch
except Exception:  # pragma: no cover - optional dependency
    OpenSearch = None  # type: ignore

logger = logging.getLogger(__name__)

MAX_LIMIT = _int_env("MAX_LIMIT", 100)
MAX_OFFSET = _int_env("MAX_OFFSET", 10_000)


def _format_pubdate(iso_str: str) -> str:
    """Return ``iso_str`` converted to RFC 2822 format.

    ``iso_str`` is expected to be an ISO 8601 timestamp.  When parsing fails a
    timestamp representing the current time is returned to satisfy RSS
    requirements.
    """

    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)


async def search_releases_async(
    client: OpenSearch,
    query: Dict[str, Any],
    *,
    limit: int,
    offset: int = 0,
    sort: str | None = None,
    api_key: str | None = None,
) -> List[Dict[str, str]]:
    """Execute ``query`` against ``client`` and return RSS-style items.

    Parameters
    ----------
    client:
        OpenSearch client instance.
    query:
        A boolean query dict to execute.
    limit / offset:
        Pagination controls.
    sort:
        Optional sort key. Accepted values are ``date`` (``posted_at``),
        ``size`` (``size_bytes``) or any raw field name. Sorting is in
        descending order.
    api_key:
        Optional API key appended to the item link.
    """
    if limit > MAX_LIMIT:
        raise ValueError("limit too high")
    if offset > MAX_OFFSET:
        offset = MAX_OFFSET
    query = dict(query)
    filters = list(query.get("filter") or [])
    filters.append({"term": {"has_parts": True}})
    query["filter"] = filters
    body: Dict[str, Any] = {
        "query": {"bool": query},
        "size": limit,
        "from": offset,
        "track_total_hits": False,
    }
    if sort:
        field_map = {
            "date": "posted_at",
            "size": "size_bytes",
            "title": "norm_title.keyword",
        }
        body["sort"] = [{field_map.get(sort, sort): {"order": "desc"}}]
    try:
        with start_span("opensearch.search"):
            result = await asyncio.to_thread(
                call_with_retry,
                os_breaker,
                "opensearch",
                client.search,
                index=OS_RELEASES_ALIAS,
                body=body,
                request_timeout=search_timeout_ms() / 1000,
            )
    except CircuitOpenError:
        logger.warning(
            "breaker_open", extra={"dep": "opensearch", "breaker_state": "open"}
        )
        return []
    except TypeError:
        try:
            result = await asyncio.to_thread(
                call_with_retry,
                os_breaker,
                "opensearch",
                client.search,
                index=OS_RELEASES_ALIAS,
                body=body,
            )
        except Exception as exc:
            logger.warning("OpenSearch search failed: %s", exc)
            return []
    except Exception as exc:
        logger.warning("OpenSearch search failed: %s", exc)
        return []
    items: List[Dict[str, str]] = []
    for hit in result.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        size = src.get("size_bytes")
        if size is None or size <= 0:
            continue

        release_id = hit.get("_id", "")
        link = f"/api?t=getnzb&id={quote(release_id, safe='')}"
        if api_key:
            link += f"&apikey={api_key}"
        items.append(
            {
                "title": src.get("norm_title", ""),
                "guid": hit.get("_id", ""),
                "pubDate": _format_pubdate(src.get("posted_at", "")),
                "category": src.get("category", ""),
                "link": link,
                "size": str(size),
            }
        )
    return items


def search_releases(
    client: OpenSearch,
    query: Dict[str, Any],
    *,
    limit: int,
    offset: int = 0,
    sort: str | None = None,
    api_key: str | None = None,
) -> List[Dict[str, str]]:
    """Synchronous wrapper for tests."""
    return asyncio.run(
        search_releases_async(
            client,
            query,
            limit=limit,
            offset=offset,
            sort=sort,
            api_key=api_key,
        )
    )
