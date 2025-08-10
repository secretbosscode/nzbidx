"""API service entrypoint using Starlette."""

import json
import logging
import os
from pathlib import Path
from typing import Optional

# Optional third party dependencies
try:  # pragma: no cover - import guard
    from opensearchpy import OpenSearch
except Exception:  # pragma: no cover - optional dependency
    OpenSearch = None  # type: ignore

try:  # pragma: no cover - import guard
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore

# Starlette (with safe fallbacks for tests/minimal envs)
try:  # pragma: no cover - import guard
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route
    from starlette.middleware import Middleware
except Exception:  # pragma: no cover - optional dependency

    class Request:  # type: ignore
        """Very small subset of Starlette's Request used for testing."""

        def __init__(self, scope: dict) -> None:
            self.query_params = scope.get("query_params", {})

    class Response:  # type: ignore
        def __init__(
            self,
            content: str,
            *,
            status_code: int = 200,
            media_type: str = "text/plain",
        ) -> None:
            self.status_code = status_code
            self.body = content.encode("utf-8")
            self.headers = {"content-type": media_type}

    class JSONResponse(Response):  # type: ignore
        def __init__(self, content: dict, *, status_code: int = 200) -> None:
            super().__init__(
                json.dumps(content),
                status_code=status_code,
                media_type="application/json",
            )

    class Route:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass

    class Middleware:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass

    class Starlette:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass


from .db import ping
from .newznab import (
    adult_content_allowed,
    adult_disabled_xml,
    caps_xml,
    get_nzb,
    is_adult_category,
    rss_xml,
    MOVIES_CAT,
    AUDIO_CAT,
    BOOKS_CAT,
    TV_CAT,
)
from .rate_limit import RateLimitMiddleware

logger = logging.getLogger(__name__)

opensearch: Optional[OpenSearch] = None
cache: Optional[Redis] = None


def init_opensearch() -> None:
    """Connect to OpenSearch and ensure indices exist."""
    global opensearch
    if OpenSearch is None:
        return
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
                "language": "en",
                "tags": ["sample"],
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
    if Redis is None:
        return
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
    tag: Optional[str] = None,
    extra: Optional[dict[str, object]] = None,
    artist: Optional[str] = None,
) -> list[dict[str, str]]:
    """Run a search against OpenSearch and return RSS item dicts."""
    items: list[dict[str, str]] = []
    if opensearch and q:
        try:
            must: list[dict[str, object]] = [{"match": {"norm_title": q}}]
            filters: list[dict[str, object]] = []

            if extra:
                for field, value in extra.items():
                    if field == "tags":
                        values = value if isinstance(value, list) else [value]
                        for v in values:
                            if v:
                                must.append({"match": {"tags": v}})
                    elif field == "year" and value:
                        try:
                            year_int = int(value)  # type: ignore[arg-type]
                            filters.append(
                                {
                                    "range": {
                                        "posted_at": {
                                            "gte": f"{year_int}-01-01",
                                            "lt": f"{year_int + 1}-01-01",
                                        }
                                    }
                                }
                            )
                        except (ValueError, TypeError):
                            pass
                    else:
                        values = value if isinstance(value, list) else [value]
                        for v in values:
                            if v:
                                must.append({"match": {field: v}})

            if artist:
                must.append(
                    {
                        "bool": {
                            "should": [
                                {"match": {"tags": artist}},
                                {"match": {"norm_title": artist}},
                            ]
                        }
                    }
                )

            if category:
                filters.append({"term": {"category": category}})

            if tag:
                # Prefix on keyword field 'tags' (requires tags to be keyword in mapping)
                filters.append({"prefix": {"tags": tag}})

            # Block adult content by default unless explicitly allowed
            must_not: list[dict[str, object]] = []
            if not adult_content_allowed():
                must_not.append({"term": {"category": "xxx"}})

            body: dict[str, dict] = {"query": {"bool": {"must": must}}}
            if filters:
                body["query"]["bool"]["filter"] = filters
            if must_not:
                body["query"]["bool"]["must_not"] = must_not

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
    cat = params.get("cat")

    # Adult category gating
    if (
        cat
        and any(is_adult_category(c.strip()) for c in cat.split(","))
        and not adult_content_allowed()
    ):
        return Response(adult_disabled_xml(), media_type="application/xml")

    if t == "caps":
        return Response(caps_xml(), media_type="application/xml")

    if t == "search":
        q = params.get("q")
        tag = params.get("tag")
        items = _os_search(q, category=cat, tag=tag)
        return Response(rss_xml(items), media_type="application/xml")

    if t == "tvsearch":
        q = params.get("q")
        season = params.get("season")
        episode = params.get("ep")
        tag = params.get("tag")
        items = _os_search(
            q,
            category=TV_CAT,
            tag=tag,
            extra={"season": season, "episode": episode},
        )
        return Response(rss_xml(items), media_type="application/xml")

    if t == "movie":
        q = params.get("q")
        imdbid = params.get("imdbid")
        tag = params.get("tag")
        items = _os_search(q, category=MOVIES_CAT, tag=tag, extra={"imdbid": imdbid})
        return Response(rss_xml(items), media_type="application/xml")

    if t == "music":
        q = params.get("q")
        artist = params.get("artist")
        tag = params.get("tag")
        extra = {
            "album": params.get("album"),
            "label": params.get("label"),
            "year": params.get("year"),
        }
        items = _os_search(
            q,
            category=AUDIO_CAT,
            tag=tag,
            extra={k: v for k, v in extra.items() if v},
            artist=artist,
        )
        return Response(rss_xml(items), media_type="application/xml")

    if t == "book":
        q = params.get("q")
        tag = params.get("tag")
        extra = {
            "author": params.get("author"),
            "year": params.get("year"),
        }
        items = _os_search(
            q,
            category=BOOKS_CAT,
            tag=tag,
            extra={k: v for k, v in extra.items() if v},
        )
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

app = Starlette(
    routes=routes,
    on_startup=[init_opensearch, init_cache],
    middleware=[Middleware(RateLimitMiddleware)],
)

if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
