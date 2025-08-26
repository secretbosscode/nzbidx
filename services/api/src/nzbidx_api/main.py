"""API service entrypoint using Starlette."""

import hashlib
import logging
import os
import sys
import time
import asyncio
from pathlib import Path
from typing import Optional, Callable

import threading
from nzbidx_ingest import ingest_loop

from .json_utils import orjson

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "nzbidx-api")


# Starlette (with safe fallbacks for tests/minimal envs)
try:  # pragma: no cover - import guard
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.routing import Route
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
except Exception:  # pragma: no cover - optional dependency

    class Request:  # type: ignore
        """Very small subset of Starlette's Request used for testing."""

        def __init__(self, scope: dict) -> None:
            self.query_params = scope.get("query_params", {})

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


from .orjson_response import ORJSONResponse, Response
from .db import (
    apply_schema,
    close_connection,
    dispose_engine,
    get_engine,
    init_engine,
    ping,
)

try:  # pragma: no cover - optional dependency
    from sqlalchemy import text
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore
from . import newznab
from .newznab import (
    caps_xml,
    get_nzb,
    NzbFetchError,
    NzbDatabaseError,
    NntpConfigError,
    NntpNoArticlesError,
    rss_xml,
    MOVIE_CATEGORY_IDS,
    TV_CATEGORY_IDS,
    AUDIO_CATEGORY_IDS,
    BOOKS_CATEGORY_IDS,
    expand_category_ids,
)
from .api_key import ApiKeyMiddleware
from .rate_limit import RateLimitMiddleware
from .middleware_quota import QuotaMiddleware
from .search_cache import cache_rss, get_cached_rss
from .search import (
    MAX_LIMIT,
    MAX_OFFSET,
    search_releases_async,
    _format_pubdate,
    SearchVectorUnavailable,
)
from .middleware_security import SecurityMiddleware
from .middleware_request_id import RequestIDMiddleware
from .middleware_circuit import CircuitOpenError, os_breaker
from .otel import current_trace_id, setup_tracing
from .errors import (
    breaker_open,
    invalid_params,
    nzb_not_found,
    nzb_timeout,
    nzb_unavailable,
    search_unavailable,
)
from .log_sanitize import LogSanitizerFilter
from .openapi import openapi_json
from .config import cors_origins, settings, reload_if_env_changed
from .metrics_log import start as start_metrics, inc_api_5xx, get_counters
from .access_log import AccessLogMiddleware
from .backfill_release_parts import backfill_release_parts

_stop_metrics: Callable[[], None] | None = None
_ingest_stop: threading.Event | None = None
_ingest_thread: threading.Thread | None = None
_backfill_thread: threading.Thread | None = None
_backfill_status: dict[str, object] = {"status": "idle", "processed": 0}
_backfill_scheduler_task: asyncio.Task | None = None

logger = logging.getLogger(__name__)

# Cache the standard LogRecord fields once so that formatters can
# efficiently filter out extra attributes on each log call.
_DEFAULT_LOG_FIELDS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)

NNTP_ERROR_MESSAGES = {
    NntpConfigError: (
        "NNTP configuration missing; set NNTP_HOST, NNTP_PORT, NNTP_USER "
        "and NNTP_PASS environment variables."
    ),
    NntpNoArticlesError: (
        "No NNTP articles found for release; verify NNTP_GROUPS and the "
        "release identifier."
    ),
}


def _backfill_progress(count: int) -> None:
    _backfill_status["processed"] = count


def _run_backfill() -> None:
    try:
        processed = backfill_release_parts(progress_cb=_backfill_progress)
        _backfill_status.update({"status": "complete", "processed": processed})
        logger.info("backfill_complete", extra={"processed": processed})
    except Exception as exc:  # pragma: no cover - defensive
        _backfill_status.update({"status": "error", "error": str(exc)})
        logger.exception("backfill_failed", exc_info=exc)


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in _DEFAULT_LOG_FIELDS:
                payload[k] = v
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return orjson.dumps(payload, default=str).decode()


class PlainFormatter(logging.Formatter):
    """Plain formatter that appends extra fields."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        base = super().format(record)
        extras = [
            f"{k}={v}"
            for k, v in record.__dict__.items()
            if k not in _DEFAULT_LOG_FIELDS
        ]
        if extras:
            if record.exc_info:
                first, *rest = base.splitlines()
                base = " ".join([first, " ".join(extras)])
                if rest:
                    base += "\n" + "\n".join(rest)
            else:
                base = " ".join([base, " ".join(extras)])
        return base


_LOG_LOCK = threading.Lock()


def setup_logging() -> None:
    root = logging.getLogger()
    if getattr(root, "_nzbidx_logging_configured", False):
        return
    with _LOG_LOCK:
        if getattr(root, "_nzbidx_logging_configured", False):
            return

        handler = logging.StreamHandler(sys.stdout)
        log_format = os.getenv("LOG_FORMAT", "plain")
        if log_format.lower() == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(
                PlainFormatter("%(asctime)s %(levelname)s %(message)s")
            )
        handler.addFilter(LogSanitizerFilter())

        root.handlers.clear()
        root.addHandler(handler)
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        root.setLevel(getattr(logging, level, logging.INFO))

        # Quiet overly chatty third-party libraries so logs stay readable.
        for name in ("urllib3", "httpx"):
            logging.getLogger(name).setLevel(logging.WARNING)

        # Forward uvicorn's access logs through the same handler without propagating.
        access = logging.getLogger("uvicorn.access")
        access.handlers.clear()
        access.propagate = False
        access.addHandler(handler)

        root._nzbidx_logging_configured = True


def _thread_excepthook(args: threading.ExceptHookArgs) -> None:
    """Log uncaught thread exceptions consistently."""
    exc = getattr(args, "exc", args.exc_value)
    name = args.thread.name if args.thread else ""
    logger.exception("thread_uncaught_exception", exc_info=exc, extra={"thread": name})


setup_logging()
threading.excepthook = _thread_excepthook
setup_tracing()

_START_TIME = time.monotonic()
INGEST_STALE_SECONDS = int(os.getenv("INGEST_STALE_SECONDS", "600"))


def start_ingest() -> None:
    global _ingest_stop, _ingest_thread
    _ingest_stop = threading.Event()
    _ingest_thread = threading.Thread(
        target=ingest_loop.run_forever, args=(_ingest_stop,), daemon=True
    )
    _ingest_thread.start()


def start_auto_backfill() -> None:
    """Launch a background thread to backfill missing release segments."""

    if os.getenv("AUTO_BACKFILL", "").lower() not in {"1", "true", "yes"}:
        logger.info("auto_backfill_disabled")
        return

    def _progress(count: int) -> None:
        logger.info("auto_backfill_progress", extra={"processed": count})

    def _run() -> None:
        try:
            processed = backfill_release_parts(auto=True, progress_cb=_progress)
            logger.info("auto_backfill_complete", extra={"processed": processed})
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("auto_backfill_failed", exc_info=exc)

    logger.info("auto_backfill_start")
    threading.Thread(target=_run, daemon=True, name="auto-backfill").start()


async def _backfill_scheduler_loop(interval: int) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(backfill_release_parts)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("scheduled_backfill_failed", exc_info=exc)
    except asyncio.CancelledError:  # pragma: no cover - cancellation
        pass


async def start_backfill_scheduler() -> None:
    """Start a background task that periodically backfills release parts."""
    global _backfill_scheduler_task
    interval = int(os.getenv("BACKFILL_INTERVAL_SECONDS", "3600"))
    if _backfill_scheduler_task is None or _backfill_scheduler_task.done():
        _backfill_scheduler_task = asyncio.create_task(
            _backfill_scheduler_loop(interval)
        )


async def stop_backfill_scheduler() -> None:
    """Stop the background backfill task if running."""
    global _backfill_scheduler_task
    if _backfill_scheduler_task:
        _backfill_scheduler_task.cancel()
        try:
            await _backfill_scheduler_task
        except Exception:  # pragma: no cover - cancellation/cleanup
            pass
        _backfill_scheduler_task = None


def stop_ingest() -> None:
    if _ingest_stop:
        _ingest_stop.set()
    if _ingest_thread:
        _ingest_thread.join(timeout=5)


def _find_version_file() -> Path:
    current = Path(__file__).resolve()
    for candidate in [current] + list(current.parents):
        path = candidate / "VERSION"
        if path.exists():
            return path
    return current


_VERSION_FILE = _find_version_file()
VERSION = os.getenv("VERSION")
if not VERSION and _VERSION_FILE.name == "VERSION":
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


class TimingMiddleware(BaseHTTPMiddleware):
    """Log timing for ``/api`` responses."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        start = time.monotonic()
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/api"):
            duration = int((time.monotonic() - start) * 1000)
            ip = request.client.host if request.client else ""
            logger.info(
                "request",
                extra={
                    "service": SERVICE_NAME,
                    "route": path,
                    "status": response.status_code,
                    "duration_ms": duration,
                    "ip": ip,
                    "trace_id": current_trace_id(),
                    "request_id": request.state.request_id,
                },
            )
        if response.status_code >= 500:
            inc_api_5xx()
        return response


async def health(request: Request) -> ORJSONResponse:
    """Health check endpoint."""
    db_status = "ok" if await ping() else "down"
    req_id = getattr(request.state, "request_id", "")
    payload = {"status": "ok", "db": db_status, "request_id": req_id}
    last = getattr(ingest_loop, "last_run", 0.0)
    payload["ingest_last_run"] = int(last)
    age = time.time() - last if last else None
    payload["ingest_age_seconds"] = int(age) if age is not None else None
    if age is None or age > INGEST_STALE_SECONDS:
        payload["ingest"] = "stale"
        payload["status"] = "warn"
    else:
        payload["ingest"] = "ok"
    payload["version"] = VERSION or "dev"
    payload["uptime_ms"] = int((time.monotonic() - _START_TIME) * 1000)
    payload["build"] = BUILD
    return ORJSONResponse(payload)


async def status(request: Request) -> ORJSONResponse:
    """Return dependency status and circuit breaker states."""
    req_id = getattr(request.state, "request_id", "")
    payload = {"request_id": req_id, "breaker": {"os": await os_breaker.state()}}
    return ORJSONResponse(payload)


async def metrics(request: Request) -> ORJSONResponse:
    """Expose internal metrics counters."""
    return ORJSONResponse(get_counters())


async def config_endpoint(request: Request) -> ORJSONResponse:
    """Expose effective timeout configuration values."""
    payload = {
        "nzb_timeout_seconds": settings.nzb_timeout_seconds,
        "nntp_total_timeout_seconds": settings.nntp_total_timeout_seconds,
    }
    return ORJSONResponse(payload)


async def admin_backfill(request: Request) -> ORJSONResponse:
    """Trigger or query a background backfill of release parts."""
    global _backfill_thread
    if _backfill_thread and _backfill_thread.is_alive():
        return ORJSONResponse(
            {"status": "running", "processed": _backfill_status.get("processed", 0)}
        )
    if _backfill_status["status"] in {"complete", "error"}:
        return ORJSONResponse(_backfill_status)
    _backfill_status.update({"status": "running", "processed": 0})
    logger.info("backfill_started")
    _backfill_thread = threading.Thread(target=_run_backfill, daemon=True)
    _backfill_thread.start()
    return ORJSONResponse({"status": "started"})


async def ensure_search_vector() -> None:
    """Verify required ``search_vector`` column exists."""

    engine = get_engine()
    if not engine or text is None:  # pragma: no cover - dependency check
        return
    async with engine.connect() as conn:
        exists = await conn.scalar(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name='release'
                  AND column_name='search_vector'
                """
            )
        )
    if not exists:
        msg = (
            "search_vector column missing; apply schema.sql to initialize the database"
        )
        logger.error(msg)
        raise RuntimeError(msg)


async def _search(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    extra: Optional[dict[str, object]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = None,
    api_key: Optional[str] = None,
) -> list[dict[str, str]]:
    """Run a database search and return RSS item dicts."""
    q = q.strip() if isinstance(q, str) else None
    try:
        if not get_engine():
            raise RuntimeError("database engine not initialized")
        return await search_releases_async(
            q,
            category=category,
            tag=tag,
            limit=limit,
            offset=offset,
            sort=sort,
            api_key=api_key,
        )
    except Exception as exc:
        logger.exception("search_failed", exc_info=exc)
        raise


def _xml_response(body: str) -> Response:
    """Return ``body`` as an XML response."""
    return Response(body, media_type="application/xml")


def _cached_xml_response(
    request: Request, body: str, *, allow_304: bool = True
) -> Response:
    """Return ``body`` with caching headers and optional 304 support."""
    etag = hashlib.sha1(body.encode("utf-8")).hexdigest()
    headers = {
        "Cache-Control": f"public, max-age={settings.search_ttl_seconds}",
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
    api_key = params.get("apikey")
    raw_qs = getattr(request, "query_string", None)
    if isinstance(raw_qs, (bytes, bytearray)):
        qs_len = len(raw_qs)
    else:
        qs_len = sum(len(k) + len(v) + 1 for k, v in params.items())
    if qs_len > settings.max_query_bytes:
        return invalid_params("query string too long")
    for value in params.values():
        if value and len(value) > settings.max_param_bytes:
            return invalid_params("invalid parameters")
    t = params.get("t")
    cat = params.get("cat")
    no_cache = request.headers.get("Cache-Control") == "no-cache"

    try:
        limit = int(params.get("limit", "") or 50)
    except ValueError:
        limit = 50
    if limit > MAX_LIMIT:
        return invalid_params("limit too high")
    try:
        offset = int(params.get("offset", "0"))
    except ValueError:
        offset = 0
    if offset > MAX_OFFSET:
        offset = MAX_OFFSET
    sort = params.get("sort")
    extended = params.get("extended") == "1"

    if cat:
        cats = [c.strip() for c in cat.split(",") if c.strip()]
        cats = expand_category_ids(cats)
        cat = ",".join(cats) if cats else None

    if t == "caps":
        return _xml_response(caps_xml())

    if t == "search":
        cache_key = f"search:{_params_key(params)}"
        cached = await get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return invalid_params("query too long")
        tag = params.get("tag")
        try:
            items = await _search(
                q,
                category=cat,
                tag=tag,
                limit=limit,
                offset=offset,
                sort=sort,
                api_key=api_key,
            )
        except SearchVectorUnavailable as exc:
            return search_unavailable(str(exc), status_code=503)
        except Exception:
            return search_unavailable()
        xml = rss_xml(items, extended=extended)
        if not no_cache:
            await cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "tvsearch":
        cache_key = f"tvsearch:{_params_key(params)}"
        cached = await get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return invalid_params("query too long")
        season = params.get("season")
        episode = params.get("ep")
        tag = params.get("tag")
        cats = cat or ",".join(TV_CATEGORY_IDS)
        try:
            items = await _search(
                q,
                category=cats,
                tag=tag,
                extra={"season": season, "episode": episode},
                limit=limit,
                offset=offset,
                sort=sort,
                api_key=api_key,
            )
        except SearchVectorUnavailable as exc:
            return search_unavailable(str(exc), status_code=503)
        except Exception:
            return search_unavailable()
        xml = rss_xml(items, extended=extended)
        if not no_cache:
            await cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "movie":
        cache_key = f"movie:{_params_key(params)}"
        cached = await get_cached_rss(cache_key) if not no_cache else None
        if cached:
            return _cached_xml_response(request, cached)
        q = params.get("q")
        if q and len(q) > 256:
            return invalid_params("query too long")
        imdbid = params.get("imdbid")
        tag = params.get("tag")
        cats = cat or ",".join(MOVIE_CATEGORY_IDS)
        try:
            items = await _search(
                q,
                category=cats,
                tag=tag,
                extra={"imdbid": imdbid, "resolution": params.get("resolution")},
                limit=limit,
                offset=offset,
                sort=sort,
                api_key=api_key,
            )
        except SearchVectorUnavailable as exc:
            return search_unavailable(str(exc), status_code=503)
        except Exception:
            return search_unavailable()
        if not q and not items:
            first_cat = cats.split(",")[0] if cats else MOVIE_CATEGORY_IDS[0]
            link = "/api?t=getnzb&id=0"
            if api_key:
                link += f"&apikey={api_key}"
            items = [
                {
                    "title": "Indexer Test Item",
                    "guid": "0",
                    "pubDate": _format_pubdate(None),
                    "category": first_cat,
                    "link": link,
                    "size": "1",
                }
            ]
        xml = rss_xml(items, extended=extended)
        if not no_cache:
            await cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "music":
        cache_key = f"music:{_params_key(params)}"
        cached = await get_cached_rss(cache_key) if not no_cache else None
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
        cats = cat or ",".join(AUDIO_CATEGORY_IDS)
        try:
            items = await _search(
                q,
                category=cats,
                tag=tag,
                extra=extra,
                limit=limit,
                offset=offset,
                sort=sort,
                api_key=api_key,
            )
        except SearchVectorUnavailable as exc:
            return search_unavailable(str(exc), status_code=503)
        except Exception:
            return search_unavailable()
        xml = rss_xml(items, extended=extended)
        if not no_cache:
            await cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "book":
        cache_key = f"book:{_params_key(params)}"
        cached = await get_cached_rss(cache_key) if not no_cache else None
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
        cats = cat or ",".join(BOOKS_CATEGORY_IDS)
        try:
            items = await _search(
                q,
                category=cats,
                tag=tag,
                extra=extra,
                limit=limit,
                offset=offset,
                sort=sort,
                api_key=api_key,
            )
        except SearchVectorUnavailable as exc:
            return search_unavailable(str(exc), status_code=503)
        except Exception:
            return search_unavailable()
        xml = rss_xml(items, extended=extended)
        if not no_cache:
            await cache_rss(cache_key, xml)
        return _cached_xml_response(request, xml, allow_304=not no_cache)

    if t == "getnzb":
        release_id = params.get("id")
        if not release_id:
            return invalid_params("missing id")
        logger.info("fetching nzb", extra={"release_id": release_id})
        start = time.perf_counter()
        try:
            xml = await asyncio.wait_for(
                get_nzb(release_id, None),
                timeout=settings.nzb_timeout_seconds,
            )
            duration_ms = int((time.perf_counter() - start) * 1000)
            logger.info(
                "nzb fetched",
                extra={"release_id": release_id, "duration_ms": duration_ms},
            )
        except CircuitOpenError:
            return breaker_open()
        except NzbDatabaseError as exc:
            logger.error(
                "nzb database query failed",
                extra={"release_id": release_id, "error": str(exc)},
            )
            return nzb_unavailable("database query failed")
        except NzbFetchError as exc:
            msg = NNTP_ERROR_MESSAGES.get(type(exc), str(exc))
            logger.warning(
                "nzb fetch failed: %s",
                msg,
                extra={"release_id": release_id, "error": str(exc)},
            )
            return nzb_not_found(f"No segments found for release {release_id}")
        except asyncio.TimeoutError:
            logger.warning(
                "nzb fetch timed out after %ss",
                settings.nzb_timeout_seconds,
                extra={"release_id": release_id},
            )
            resp = nzb_timeout("nzb fetch timed out")
            resp.headers["Retry-After"] = str(newznab.FAIL_TTL)
            return resp
        return Response(
            xml,
            media_type="application/x-nzb",
            headers={"Content-Disposition": f'attachment; filename="{release_id}.nzb"'},
        )

    return invalid_params("unsupported request")


routes = [
    Route("/health", health),
    Route("/api/health", health),
    Route("/api/status", status),
    Route("/api/metrics", metrics),
    Route("/api/config", config_endpoint),
    Route("/api/admin/backfill", admin_backfill, methods=["POST"]),
    Route("/api", api),
    Route("/openapi.json", openapi_json),
]
middleware = [
    Middleware(RequestIDMiddleware),
    Middleware(ApiKeyMiddleware),
    Middleware(QuotaMiddleware),
    Middleware(RateLimitMiddleware),
    Middleware(SecurityMiddleware, max_request_bytes=settings.max_request_bytes),
    Middleware(TimingMiddleware),
    Middleware(AccessLogMiddleware),
]
origins = cors_origins()
if origins:
    middleware.append(Middleware(CORSMiddleware, allow_origins=origins))

app = Starlette(
    routes=routes,
    on_startup=[
        reload_if_env_changed,
        init_engine,
        apply_schema,
        ensure_search_vector,
        start_ingest,
        start_auto_backfill,
        lambda: _set_stop(start_metrics()),
    ],
    on_shutdown=[
        stop_ingest,
        lambda: _stop_metrics() if _stop_metrics else None,
        dispose_engine,
        close_connection,
    ],
    middleware=middleware,
)


def _set_stop(cb):
    global _stop_metrics
    _stop_metrics = cb


if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        loop="asyncio",
        http="h11",
        access_log=False,
    )
