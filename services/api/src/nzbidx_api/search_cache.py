"""Helpers for caching RSS search responses in Redis."""

from __future__ import annotations

from typing import Optional

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
        value = client.get(f"rss:{key}")
        if value:
            return value.decode("utf-8")
    return None


def cache_rss(key: str, xml: str, ttl: int = 60) -> None:
    """Store ``xml`` under ``key`` for ``ttl`` seconds if caching is enabled."""
    client = _client()
    if client:
        client.setex(f"rss:{key}", ttl, xml)
