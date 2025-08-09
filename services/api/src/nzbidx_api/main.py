"""API service entrypoint using Starlette."""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from opensearchpy import OpenSearch
from redis import Redis
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .db import ping
from .newznab import caps_xml, get_nzb, rss_xml

logger = logging.getLogger(__name__)

opensearch: Optional[OpenSearch] = None
cache: Optional[Redis] = None


def init_opensearch() -> None:
    """Connect to OpenSearch and ensure indices exist."""
    global opensearch
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        client = OpenSearch(url, timeout=2)
        template_path = (
            Path(__file__).resolve().parents[3] / "opensearch" / "index-template.json"
        )
        with template_path.open("r", encoding="utf-8") as f:
            template_body = json.load(f)
        client.indices.put_index_template(
            name="nzbidx-releases-template", body=template_body
        )
        if not client.indices.exists(index="nzbidx-releases-v1"):
            client.indices.create(index="nzbidx-releases-v1")
        if os.getenv("SEED_OS_SAMPLE") == "true":
            sample = {
                "norm_title": "Test Release",
                "category": "test",
                "posted_at": "1970-01-01T00:00:00Z",
                "size_bytes": 0,
            }
            try:
                client.index(
                    index="nzbidx-releases-v1", id="1", body=sample, refresh=True
                )
            except Exception:
                pass
        opensearch = client
        logger.info("OpenSearch ready")
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("OpenSearch unavailable: %s", exc)


def init_cache() -> None:
    """Connect to Redis for caching NZB documents."""
    global cache
    url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    try:
        client = Redis.from_url(url)
        client.ping()
        cache = client
        logger.info("Redis ready")
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("Redis unavailable: %s", exc)


async def health(request: Request) -> JSONResponse:
    """Health check endpoint."""
    db_status = "ok" if await ping() else "down"
    return JSONResponse({"status": "ok", "db": db_status})


def _os_search(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    extra: Optional[dict[str, str]] = None,
) -> list[dict[str, str]]:
    """Run a search against OpenSearch and return RSS item dicts.

    ``q`` is the user query. ``category`` restricts results to a specific
    Newznab category. ``extra`` allows additional field matches, e.g. season or
    imdbid. Missing OpenSearch or errors simply result in an empty list.
    """

    items: list[dict[str, str]] = []
    if opensearch and q:
        try:
            must = [{"match": {"norm_title": q}}]
            if extra:
                for field, value in extra.items():
                    if value:
                        must.append({"match": {field: value}})
            body: dict[str, dict] = {"query": {"bool": {"must": must}}}
            if category:
                body["query"]["bool"].setdefault("filter", []).append(
                    {"term": {"category": category}}
                )
            result = opensearch.search(index="nzbidx-releases-v1", body=body)
            for hit in result.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                items.append(
                    {
                        "title": src.get("norm_title", ""),
                        "guid": hit.get("_id", ""),
                        "pubDate": src.get("posted_at", ""),
                        "category": src.get("category", ""),
                        "link": f"/api?t=getnzb&id={hit.get('_id','')}",
                    }
                )
        except Exception:
            items = []
    return items


async def api(request: Request) -> Response:
    """Newznab compatible endpoint."""
    params = request.query_params
    t = params.get("t")
    if t == "caps":
        return Response(caps_xml(), media_type="application/xml")

    if t == "search":
        q = params.get("q")
        items = _os_search(q)
        return Response(rss_xml(items), media_type="application/xml")

    if t == "tvsearch":
        q = params.get("q")
        season = params.get("season")
        episode = params.get("ep")
        items = _os_search(
            q,
            category="5000",
            extra={"season": season, "episode": episode},
        )
        return Response(rss_xml(items), media_type="application/xml")

    if t == "movie":
        q = params.get("q")
        imdbid = params.get("imdbid")
        items = _os_search(q, category="2000", extra={"imdbid": imdbid})
        return Response(rss_xml(items), media_type="application/xml")

    if t == "getnzb":
        release_id = params.get("id")
        if not release_id:
            return JSONResponse({"detail": "missing id"}, status_code=400)
        return Response(get_nzb(release_id, cache), media_type="application/x-nzb")

    return JSONResponse({"detail": "unsupported request"}, status_code=400)


routes = [
    Route("/health", health),
    Route("/api", api),
]

app = Starlette(routes=routes, on_startup=[init_opensearch, init_cache])

if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
