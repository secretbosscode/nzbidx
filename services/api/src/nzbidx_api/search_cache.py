"""Helpers for caching RSS search responses in memory."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional, Union

from cachetools import TTLCache

from .config import settings


def _new_cache(timer: Callable[[], float] = time.monotonic) -> TTLCache[str, bytes]:
    """Create a TTL cache with the configured limits."""

    return TTLCache(
        maxsize=settings.search_cache_max_entries,
        ttl=settings.search_ttl_seconds,
        timer=timer,
    )


# In-memory cache with automatic TTL and LRU eviction
_CACHE: TTLCache[str, bytes] = _new_cache()

# Guard access to ``_CACHE`` so readers/writers don't interfere with each other
_CACHE_LOCK = asyncio.Lock()

PURGE_INTERVAL = 30
_LAST_PURGE = 0.0

logger = logging.getLogger(__name__)


def _ensure_cache_config() -> None:
    """Reload the cache if configuration settings have changed."""

    global _CACHE
    if (
        _CACHE.ttl != settings.search_ttl_seconds
        or _CACHE.maxsize != settings.search_cache_max_entries
    ):
        _CACHE = _new_cache()


def _purge_expired_locked(now: Optional[float] = None) -> None:
    """Internal helper that prunes expired entries while the cache lock is held."""

    global _LAST_PURGE

    current = now if now is not None else time.monotonic()
    if current - _LAST_PURGE >= PURGE_INTERVAL:
        _CACHE.expire(current)
        _LAST_PURGE = current


async def purge_expired() -> None:
    """Delete any expired cache entries."""
    async with _CACHE_LOCK:
        _ensure_cache_config()
        _purge_expired_locked()


async def get_cached_rss(key: str) -> Optional[bytes]:
    """Return cached RSS XML for ``key`` if present and not expired."""
    async with _CACHE_LOCK:
        _ensure_cache_config()
        now = time.monotonic()
        _purge_expired_locked(now)
        try:
            xml = _CACHE[key]
        except KeyError:
            return None
        logger.info("search_cache_hit", extra={"key": key})
        return xml


async def cache_rss(key: str, xml: Union[str, bytes]) -> None:
    """Store ``xml`` under ``key`` using the configured TTL."""
    async with _CACHE_LOCK:
        _ensure_cache_config()
        _purge_expired_locked(time.monotonic())
        xml_bytes = xml.encode("utf-8") if isinstance(xml, str) else xml
        if b"<item>" not in xml_bytes:
            return
        _CACHE[key] = xml_bytes
