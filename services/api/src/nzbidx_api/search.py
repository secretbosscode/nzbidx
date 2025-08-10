"""OpenSearch query helpers."""

from __future__ import annotations

from typing import Any, Dict, List

import logging

try:  # pragma: no cover - optional dependency
    from opensearchpy import OpenSearch
except Exception:  # pragma: no cover - optional dependency
    OpenSearch = None  # type: ignore

logger = logging.getLogger(__name__)


def search_releases(
    client: OpenSearch,
    query: Dict[str, Any],
    *,
    limit: int,
    offset: int = 0,
) -> List[Dict[str, str]]:
    """Execute ``query`` against ``client`` and return RSS-style items."""
    body = {
        "query": {"bool": query},
        "size": limit,
        "from": offset,
        "track_total_hits": False,
    }
    try:
        result = client.search(index="nzbidx-releases-v1", body=body, request_timeout=2)
    except TypeError:
        try:
            result = client.search(index="nzbidx-releases-v1", body=body)
        except Exception as exc:
            logger.warning("OpenSearch search failed: %s", exc)
            return []
    except Exception as exc:
        logger.warning("OpenSearch search failed: %s", exc)
        return []
    items: List[Dict[str, str]] = []
    for hit in result.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        items.append(
            {
                "title": src.get("norm_title", ""),
                "guid": hit.get("_id", ""),
                "pubDate": src.get("posted_at", ""),
                "category": src.get("category", ""),
                "link": f"/api?t=getnzb&id={hit.get('_id', '')}",
            }
        )
    return items
