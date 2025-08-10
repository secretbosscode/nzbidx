"""Helpers for caching RSS search responses in Redis."""

from __future__ import annotations

from typing import Optional

from .middleware_circuit import CircuitOpenError, call_with_retry, redis_breaker
from .otel import start_span

try:  # pragma: no cover - optional dependency
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore


def _client() -> Optional[Redis]:
    """Return the Redis cache client from ``nzbidx_api.main`` if available."""
    from . import main

    return getattr(main, "cache", None)


def get_cached_rss(key: str) -> Optional[str]:
    """Return a cached RSS XML document for ``key`` if present."""
    client = _client()
    if client:
        try:
            with start_span("redis.get"):
                value = call_with_retry(
                    redis_breaker, "redis", client.get, f"rss:{key}"
                )
            if value:
                return value.decode("utf-8")
        except CircuitOpenError:
            return None
    return None


def cache_rss(key: str, xml: str) -> None:
    """Store ``xml`` under ``key`` if caching is enabled."""
    client = _client()
    if client:
        from .config import search_ttl_seconds

        try:
            with start_span("redis.setex"):
                call_with_retry(
                    redis_breaker,
                    "redis",
                    client.setex,
                    f"rss:{key}",
                    search_ttl_seconds(),
                    xml,
                )
        except CircuitOpenError:
            return
