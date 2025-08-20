"""Helpers for caching RSS search responses in memory."""

from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

# Simple in-memory cache mapping keys to (expiry, xml)
_CACHE: Dict[str, Tuple[float, str]] = {}


def purge_expired() -> None:
    """Delete any expired cache entries."""
    now = time.time()
    for key, (expires, _) in list(_CACHE.items()):
        if expires < now:
            del _CACHE[key]


async def get_cached_rss(key: str) -> Optional[str]:
    """Return cached RSS XML for ``key`` if present and not expired."""
    purge_expired()
    entry = _CACHE.get(key)
    if not entry:
        return None
    expires, xml = entry
    if expires < time.time():
        # Drop stale entry
        del _CACHE[key]
        return None
    return xml


async def cache_rss(key: str, xml: str) -> None:
    """Store ``xml`` under ``key`` using the configured TTL."""
    from .config import search_ttl_seconds

    purge_expired()
    _CACHE[key] = (time.time() + search_ttl_seconds(), xml)
