"""OpenSearch query helpers."""

from __future__ import annotations

from typing import Any, Dict, List

try:  # pragma: no cover - optional dependency
    from opensearchpy import OpenSearch
except Exception:  # pragma: no cover - optional dependency
    OpenSearch = None  # type: ignore


def search_releases(client: OpenSearch, query: Dict[str, Any]) -> List[Dict[str, str]]:
    """Execute ``query`` against ``client`` and return RSS-style items."""
    try:
        result = client.search(
            index="nzbidx-releases-v1", body={"query": {"bool": query}}
        )
    except Exception:
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
