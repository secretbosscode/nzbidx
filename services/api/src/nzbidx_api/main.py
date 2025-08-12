"""API service entrypoint using Starlette."""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional, Callable

from nzbidx_common.os import OS_RELEASES_ALIAS

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
        def __init__(
            self, path: str, endpoint: Callable, methods: Optional[list[str]] = None
        ) -> None:
            """Minimal route container used when Starlette isn't available."""
            self.path = path
            self.endpoint = endpoint
            self.methods = methods or ["GET"]

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
        def __init__(
            self,
            *,
            routes: Optional[list[Route]] = None,
            on_startup: Optional[list[Callable]] = None,
            on_shutdown: Optional[list[Callable]] = None,
            middleware: Optional[list[Middleware]] = None,
        ) -> None:
            """Store basic application configuration for tests.

            This stub only keeps track of routes so that a lightweight test
            client can dispatch to the correct endpoint.  The full ASGI
            interface and middleware handling provided by Starlette are far
            beyond the needs of the smoke tests, so they are intentionally
            omitted.
            """
            self.routes = routes or []
            self.on_startup = on_startup or []
            self.on_shutdown = on_shutdown or []
            self.middleware = middleware or []


from .db import ping, apply_schema
from .newznab import (
    adult_content_allowed,
    adult_disabled_xml,
    caps_xml,
    get_nzb,
    NzbFetchError,
    is_adult_category,
    rss_xml,
    MOVIES_CAT,
    AUDIO_CAT,
    BOOKS_CAT,
    TV_CAT,
)
from .api_key import ApiKeyMiddleware
from .rate_limit import RateLimitMiddleware
from .middleware_quota import QuotaMiddleware
from .search_cache import cache_rss, get_cached_rss
from .search import search_releases
from .middleware_security import SecurityMiddleware
from .middleware_request_id import RequestIDMiddleware
from .middleware_circuit import CircuitOpenError, os_breaker, redis_breaker
from .otel import current_trace_id, setup_tracing
from .errors import invalid_params, breaker_open, nzb_unavailable
from .log_sanitize import LogSanitizerFilter
from .openapi import openapi_json
from .config import (
    cors_origins,
    max_request_bytes,
    max_query_bytes,
    max_param_bytes,
    search_ttl_seconds,
    os_primary_shards,
    os_replicas,
)
from .metrics_log import start as start_metrics, inc_api_5xx

_stop_metrics: Callable[[], None] | None = None

logger = logging.getLogger(__name__)


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in logging.LogRecord("", 0, "", 0, "", (), None).__dict__:
                payload[k] = v
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.addFilter(LogSanitizerFilter())
    root.setLevel(logging.INFO)


setup_logging()
setup_tracing()

_START_TIME = time.monotonic()
_VERSION_FILE = Path(__file__).resolve().parents[3] / "VERSION"
VERSION = os.getenv("VERSION")
if not VERSION and _VERSION_FILE.exists():
    VERSION = _VERSION_FILE.read_text(encoding="utf-8").strip()


def _git_sha() -> str:
    try:
        import subprocess

        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=_VERSION_FILE.parent,
            )
            .decode()
            .strip()
        )
    except Exception:  # pragma: no cover - git not available
        return ""


BUILD = os.getenv("GIT_SHA", _git_sha())

opensearch: Optional[OpenSearch] = None
cache: Optional[Redis] = None


def build_ilm_policy() -> dict[str, object]:
    path = Path(__file__).resolve().parents[4] / "opensearch" / "ilm-policy.json"
    with path.open("r", encoding="utf-8") as f:
        policy = json.load(f)
    from .config import ilm_delete_days, ilm_warm_days

    policy["policy"]["phases"]["warm"]["min_age"] = f"{ilm_warm_days()}d"
    policy["policy"]["phases"]["delete"]["min_age"] = f"{ilm_delete_days()}d"
    return policy


def build_index_template() -> dict[str, object]:
    path = Path(__file__).resolve().parents[4] / "opensearch" / "index-template.json"
    with path.open("r", encoding="utf-8") as f:
        template = json.load(f)
    settings = template.setdefault("template", {}).setdefault("settings", {})
    settings.setdefault("refresh_interval", "5s")
    settings["index.lifecycle.name"] = "nzbidx-releases-policy"
    settings["index.lifecycle.rollover_alias"] = OS_RELEASES_ALIAS
    settings["number_of_shards"] = os_primary_shards()
    settings["number_of_replicas"] = os_replicas()
    return template


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
                    "service": os.getenv("OTEL_SERVICE_NAME", "nzbidx-api"),
                    "route": path,
                    "status": response.status_code,
                    "duration_ms": duration,
                    "ip": ip,
                    "trace_id": current_trace_id(),
                    "request_id": getattr(request.state, "request_id", ""),
                },
            )
        if response.status_code >= 500:
            inc_api_5xx()
        return response


def init_opensearch() -> None:
    """Connect to OpenSearch and ensure indices exist."""
    global opensearch
    if OpenSearch is None:
        return
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        client = OpenSearch(url, timeout=2)
        client.ilm.put_lifecycle(name="nzbidx-releases-policy", body=build_ilm_policy())
        client.indices.put_index_template(
            name="nzbidx-releases-template", body=build_index_template()
        )
        try:
            alias_info = client.indices.get_alias(name=OS_RELEASES_ALIAS)
            if not any(
                d["aliases"].get(OS_RELEASES_ALIAS, {}).get("is_write_index")
                for d in alias_info.values()
            ):
                index_name = next(iter(alias_info))
                client.indices.put_alias(
                    index=index_name, name=OS_RELEASES_ALIAS, is_write_index=True
                )
        except Exception:
            initial_index = f"{OS_RELEASES_ALIAS}-000001"
            if not client.indices.exists(index=initial_index):
                client.indices.create(
                    index=initial_index,
                    aliases={OS_RELEASES_ALIAS: {"is_write_index": True}},
                )
            else:
                client.indices.put_alias(
                    index=initial_index, name=OS_RELEASES_ALIAS, is_write_index=True
                )
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
                    index=OS_RELEASES_ALIAS,
                    id="1",
                    body=sample,
                    refresh="wait_for",
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


async def shutdown() -> None:
    """Close global connections on shutdown."""
    global opensearch, cache
    if opensearch:
        try:
            opensearch.close()  # type: ignore[attr-defined]
        except Exception:
            pass
        opensearch = None
    if cache:
        try:
            cache.close()  # type: ignore[attr-defined]
        except Exception:
            pass
        cache = None


async def health(request: Request) -> JSONResponse:
    """Health check endpoint."""
    db_status = "ok" if await ping() else "down"
    req_id = getattr(getattr(request, "state", object()), "request_id", "")
    payload = {"status": "ok", "db": db_status, "request_id": req_id}
    if opensearch:
        start = time.monotonic()
        try:  # pragma: no cover - network errors
            opensearch.info()
            payload["os"] = "ok"
        except Exception:
            payload["os"] = "down"
        payload["os_latency_ms"] = int((time.monotonic() - start) * 1000)
    else:
        payload["os"] = "down"
        payload["os_latency_ms"] = 0
    if cache:
        start = time.monotonic()
        try:  # pragma: no cover - network errors
            cache.ping()
            payload["redis"] = "ok"
        except Exception:
            payload["redis"] = "down"
        payload["redis_latency_ms"] = int((time.monotonic() - start) * 1000)
    else:
        payload["redis"] = "down"
        payload["redis_latency_ms"] = 0
    payload["version"] = VERSION or "dev"
    payload["uptime_ms"] = int((time.monotonic() - _START_TIME) * 1000)
    payload["build"] = BUILD
    return JSONResponse(payload)


async def status(request: Request) -> JSONResponse:
    """Return dependency status and circuit breaker states."""
    req_id = getattr(getattr(request, "state", object()), "request_id", "")
    payload = {"request_id": req_id, "breaker": {}}
    if opensearch:
        try:  # pragma: no cover - network errors
            opensearch.info()
            payload["os"] = "ok"
        except Exception:
            payload["os"] = "down"
    else:
        payload["os"] = "down"
    if cache:
        try:  # pragma: no cover - network errors
            cache.ping()
            payload["redis"] = "ok"
        except Exception:
            payload["redis"] = "down"
    else:
        payload["redis"] = "down"
    payload["breaker"]["os"] = os_breaker.state()
    payload["breaker"]["redis"] = redis_breaker.state()
    return JSONResponse(payload)


async def admin_takedown(request: Request) -> JSONResponse:
    """Remove a release from the search index."""
    if opensearch is None:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    try:
        data = await request.json()
    except Exception:
        data = {}
    release_id = data.get("id") or request.query_params.get("id")
    if not release_id:
        return invalid_params("missing id")
    try:
        opensearch.delete(  # type: ignore[union-attr]
            index=OS_RELEASES_ALIAS, id=release_id, refresh="wait_for"
        )
    except Exception as exc:
        logger.warning("takedown_failed", extra={"id": release_id, "error": str(exc)})
        return JSONResponse({"status": "error"}, status_code=500)
    return JSONResponse({"status": "ok"})


def _os_search(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    extra: Optional[dict[str, object]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = None,
) -> list[dict[str, str]]:
    """Run a search against OpenSearch and return RSS item dicts."""
    items: list[dict[str, str]] = []
    if opensearch and q:
        try:
            must: list[dict[str, object]] = [
                {"match": {"norm_title": {"query": q, "fuzziness": "AUTO"}}}
            ]
            should: list[dict[str, object]] = [
                {"match": {"tags": {"query": q, "boost": 2}}}
            ]
            filters: list[dict[str, object]] = []

            tag_fields = {
                "artist",
                "album",
                "author",
                "title",
                "format",
                "bitrate",
                "isbn",
                "studio",
                "site",
                "resolution",
            }

            if extra:
                for field, value in extra.items():
                    if field == "year" and value:
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
                        continue

                    values = value if isinstance(value, list) else [value]
                    for v in values:
                        if not v:
                            continue
                        if field == "tags" or field in tag_fields:
                            must.append({"term": {"tags": str(v).lower()}})
                        else:
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
            if should:
                query["should"] = should
                query["minimum_should_match"] = 0
            items = search_releases(
                opensearch, query, limit=limit, offset=offset, sort=sort
            )
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
    raw_qs = getattr(request, "query_string", None)
    if isinstance(raw_qs, (bytes, bytearray)):
        qs_len = len(raw_qs)
    else:
        qs_len = sum(len(k) + len(v) + 1 for k, v in params.items())
    if qs_len > max_query_bytes():
        return invalid_params("query string too long")
    for value in params.values():
        if value and len(value) > max_param_bytes():
            return invalid_params("invalid parameters")
    t = params.get("t")
    cat = params.get("cat")
    no_cache = request.headers.get("Cache-Control") == "no-cache"

    try:
        limit = int(params.get("limit", "") or 50)
    except ValueError:
        limit = 50
    if limit > 100:
        return invalid_params("limit too high")
    try:
        offset = int(params.get("offset", "0"))
    except ValueError:
        offset = 0
    sort = params.get("sort")

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
            return invalid_params("query too long")
        tag = params.get("tag")
        items = _os_search(
            q, category=cat, tag=tag, limit=limit, offset=offset, sort=sort
        )
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
            return invalid_params("query too long")
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
            sort=sort,
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
            return invalid_params("query too long")
        imdbid = params.get("imdbid")
        tag = params.get("tag")
        items = _os_search(
            q,
            category=MOVIES_CAT,
            tag=tag,
            extra={"imdbid": imdbid, "resolution": params.get("resolution")},
            limit=limit,
            offset=offset,
            sort=sort,
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
            return invalid_params("query too long")
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
            sort=sort,
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
            return invalid_params("query too long")
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
            sort=sort,
        )
        xml = rss_xml(items)
        if not no_cache:
            cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "getnzb":
        release_id = params.get("id")
        if not release_id:
            return invalid_params("missing id")
        try:
            xml = get_nzb(release_id, cache)
        except CircuitOpenError:
            return breaker_open()
        except NzbFetchError:
            return nzb_unavailable()
        return Response(xml, media_type="application/x-nzb")

    return invalid_params("unsupported request")


routes = [
    Route("/health", health),
    Route("/api/health", health),
    Route("/api/status", status),
    Route("/api/admin/takedown", admin_takedown, methods=["POST"]),
    Route("/api", api),
    Route("/openapi.json", openapi_json),
]
middleware = [
    Middleware(RequestIDMiddleware),
    Middleware(ApiKeyMiddleware),
    Middleware(QuotaMiddleware),
    Middleware(RateLimitMiddleware),
    Middleware(SecurityMiddleware, max_request_bytes=max_request_bytes()),
    Middleware(TimingMiddleware),
]
origins = cors_origins()
if origins:
    middleware.append(Middleware(CORSMiddleware, allow_origins=origins))

app = Starlette(
    routes=routes,
    on_startup=[
        apply_schema,
        init_opensearch,
        init_cache,
        lambda: _set_stop(start_metrics()),
    ],
    on_shutdown=[shutdown, lambda: _stop_metrics() if _stop_metrics else None],
    middleware=middleware,
)


def _set_stop(cb):
    global _stop_metrics
    _stop_metrics = cb


if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
