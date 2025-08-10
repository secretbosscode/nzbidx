"""API service entrypoint using Starlette."""

import hashlib
import json
import logging
import os
import time
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
    from starlette.middleware.cors import CORSMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
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

    class CORSMiddleware:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass

    class BaseHTTPMiddleware:  # type: ignore
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
from .api_key import ApiKeyMiddleware
from .rate_limit import RateLimitMiddleware
from .search_cache import cache_rss, get_cached_rss
from .search import search_releases
from .middleware_security import SecurityMiddleware
from .config import cors_origins, max_request_bytes, search_ttl_seconds

logger = logging.getLogger(__name__)

opensearch: Optional[OpenSearch] = None
cache: Optional[Redis] = None


class TimingMiddleware(BaseHTTPMiddleware):
    """Log timing for ``/api`` responses."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        start = time.monotonic()
        response = await call_next(request)
        path = getattr(getattr(request, "url", None), "path", "")
        if path.startswith("/api"):
            duration = int((time.monotonic() - start) * 1000)
            ip = request.client.host if request.client else ""
            logger.info(
                "request",
                extra={
                    "route": path,
                    "status": response.status_code,
                    "duration_ms": duration,
                    "remote_ip": ip,
                },
            )
        return response


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
        # Speed up indexing for development environments
        template_body.setdefault("template", {}).setdefault("settings", {})[
            "refresh_interval"
        ] = "5s"
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
    payload = {"status": "ok", "db": db_status}
    if opensearch:
        start = time.monotonic()
        try:  # pragma: no cover - network errors
            opensearch.info()
            payload["os"] = "ok"
            payload["os_latency_ms"] = int((time.monotonic() - start) * 1000)
        except Exception:
            payload["os"] = "down"
    else:
        payload["os"] = "down"
    if cache:
        start = time.monotonic()
        try:  # pragma: no cover - network errors
            cache.ping()
            payload["redis"] = "ok"
            payload["redis_latency_ms"] = int((time.monotonic() - start) * 1000)
        except Exception:
            payload["redis"] = "down"
    else:
        payload["redis"] = "down"
    return JSONResponse(payload)


def _os_search(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    extra: Optional[dict[str, object]] = None,
    limit: int = 50,
    offset: int = 0,
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

            if category:
                filters.append({"term": {"category": category}})

            if tag:
                # Prefix on keyword field 'tags' (requires tags to be keyword in mapping)
                filters.append({"prefix": {"tags": tag}})

            # Block adult content by default unless explicitly allowed
            must_not: list[dict[str, object]] = []
            if not adult_content_allowed():
                must_not.append({"term": {"category": "xxx"}})

            query: dict[str, object] = {"must": must}
            if filters:
                query["filter"] = filters
            if must_not:
                query["must_not"] = must_not

            items = search_releases(opensearch, query, limit=limit, offset=offset)
        except Exception:
            items = []
    return items


def _xml_response(body: str) -> Response:
    """Return ``body`` as an XML response."""
    return Response(body, media_type="application/xml")


def _cached_xml_response(
    request: Request, body: str, *, allow_304: bool = True
) -> Response:
    """Return ``body`` with caching headers and optional 304 support."""
    etag = hashlib.sha1(body.encode("utf-8")).hexdigest()
    headers = {
        "Cache-Control": f"public, max-age={search_ttl_seconds()}",
        "ETag": etag,
    }
    if allow_304 and request.headers.get("If-None-Match") == etag:
        return Response("", status_code=304, headers=headers)
    return Response(body, media_type="application/xml", headers=headers)


def _params_key(params) -> str:
    """Return a stable cache key for ``params``."""
    return "&".join(f"{k}={v}" for k, v in sorted(params.items()))


async def api(request: Request) -> Response:
    """Newznab compatible endpoint."""
    params = request.query_params
    t = params.get("t")
    cat = params.get("cat")
    no_cache = request.headers.get("Cache-Control") == "no-cache"

    try:
        limit = int(params.get("limit", "") or 50)
    except ValueError:
        limit = 50
    if limit > 100:
        return JSONResponse({"detail": "limit too high"}, status_code=400)
    try:
        offset = int(params.get("offset", "0"))
    except ValueError:
        offset = 0

    # Adult category gating
    if (
        cat
        and any(is_adult_category(c.strip()) for c in cat.split(","))
        and not adult_content_allowed()
    ):
        return _xml_response(adult_disabled_xml())

    if t == "caps":
        return _xml_response(caps_xml())

    if t == "search":
        cache_key = f"search:{_params_key(params)}"
        cached = get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return JSONResponse({"detail": "query too long"}, status_code=400)
        tag = params.get("tag")
        items = _os_search(q, category=cat, tag=tag, limit=limit, offset=offset)
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "tvsearch":
        cache_key = f"tvsearch:{_params_key(params)}"
        cached = get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return JSONResponse({"detail": "query too long"}, status_code=400)
        season = params.get("season")
        episode = params.get("ep")
        tag = params.get("tag")
        items = _os_search(
            q,
            category=TV_CAT,
            tag=tag,
            extra={"season": season, "episode": episode},
            limit=limit,
            offset=offset,
        )
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "movie":
        cache_key = f"movie:{_params_key(params)}"
        cached = get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return JSONResponse({"detail": "query too long"}, status_code=400)
        imdbid = params.get("imdbid")
        tag = params.get("tag")
        items = _os_search(
            q,
            category=MOVIES_CAT,
            tag=tag,
            extra={"imdbid": imdbid},
            limit=limit,
            offset=offset,
        )
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "music":
        cache_key = f"music:{_params_key(params)}"
        cached = get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return JSONResponse({"detail": "query too long"}, status_code=400)
        tags = [params.get("artist"), params.get("album")]
        year = params.get("year")
        tag = params.get("tag")
        extra = {"tags": [t for t in tags if t]}
        if year:
            extra["year"] = year
        items = _os_search(
            q,
            category=AUDIO_CAT,
            tag=tag,
            extra=extra,
            limit=limit,
            offset=offset,
        )
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "book":
        cache_key = f"book:{_params_key(params)}"
        cached = get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return JSONResponse({"detail": "query too long"}, status_code=400)
        tag = params.get("tag")
        tags = [params.get("author"), params.get("title"), params.get("isbn")]
        year = params.get("year")
        extra = {"tags": [t for t in tags if t]}
        if year:
            extra["year"] = year
        items = _os_search(
            q,
            category=BOOKS_CAT,
            tag=tag,
            extra=extra,
            limit=limit,
            offset=offset,
        )
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

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
middleware = [
    Middleware(ApiKeyMiddleware),
    Middleware(RateLimitMiddleware),
    Middleware(SecurityMiddleware, max_request_bytes=max_request_bytes()),
    Middleware(TimingMiddleware),
]
origins = cors_origins()
if origins:
    middleware.append(Middleware(CORSMiddleware, allow_origins=origins))

app = Starlette(
    routes=routes,
    on_startup=[init_opensearch, init_cache],
    middleware=middleware,
)

if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
