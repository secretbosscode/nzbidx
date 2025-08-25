"""Helpers for caching RSS search responses in memory."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional, Tuple

from .config import search_ttl_seconds

# Simple in-memory cache mapping keys to (expiry, xml)
_CACHE: Dict[str, Tuple[float, str]] = {}

# Guard access to ``_CACHE`` so readers/writers don't interfere with each other
_CACHE_LOCK = asyncio.Lock()

# Cache the configured TTL once the cache is populated
_SEARCH_TTL_SECONDS: Optional[int] = None

logger = logging.getLogger(__name__)


def _purge_expired_locked(now: Optional[float] = None) -> None:
    """Internal helper that removes expired entries while the cache lock is held."""
    if now is None:
        now = time.monotonic()
    for key, (expires, _) in list(_CACHE.items()):
        if expires < now:
            del _CACHE[key]


async def purge_expired() -> None:
    """Delete any expired cache entries."""
    async with _CACHE_LOCK:
        _purge_expired_locked()


async def get_cached_rss(key: str) -> Optional[str]:
    """Return cached RSS XML for ``key`` if present and not expired."""
    async with _CACHE_LOCK:
        now = time.monotonic()
        _purge_expired_locked(now)
        entry = _CACHE.get(key)
        if not entry:
            return None
        expires, xml = entry
        if expires < now:
            # Drop stale entry
            del _CACHE[key]
            return None
        logger.info("search_cache_hit", extra={"key": key})
        return xml


async def cache_rss(key: str, xml: str) -> None:
    """Store ``xml`` under ``key`` using the configured TTL."""

    async with _CACHE_LOCK:
        _purge_expired_locked()
        if "<item>" not in xml:
            return
        global _SEARCH_TTL_SECONDS
        if not _CACHE or _SEARCH_TTL_SECONDS is None:
            _SEARCH_TTL_SECONDS = search_ttl_seconds()
        _CACHE[key] = (time.monotonic() + _SEARCH_TTL_SECONDS, xml)
