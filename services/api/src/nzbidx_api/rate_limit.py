"""Simple per-IP rate limiting middleware."""

from __future__ import annotations

import threading

from cachetools import TTLCache

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import settings
from .errors import rate_limited


class RateLimiter:
    """Track request counts per key within a time window using memory."""

    def __init__(self, limit: int, window: int, max_entries: int) -> None:
        self.limit = limit
        self.window = window
        self.counts: TTLCache[str, int] = TTLCache(maxsize=max_entries, ttl=window)
        self._lock = threading.Lock()

    async def increment(self, key: str) -> int:
        """Increment and return current count for ``key``."""
        with self._lock:
            count = self.counts.get(key, 0) + 1
            self.counts[key] = count
            return count


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Apply simple IP based rate limiting."""

    def __init__(
        self,
        app,
        limit: int | None = None,
        window: int | None = None,
        max_entries: int | None = None,
    ) -> None:
        super().__init__(app)
        limit_val = limit if limit is not None else settings.rate_limit
        window_val = window if window is not None else settings.rate_window
        max_entries_val = (
            max_entries if max_entries is not None else settings.rate_limit_max_ips
        )
        self.limiter = RateLimiter(limit_val, window_val, max_entries_val)
        self.limit = limit_val

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = request.client.host if request.client else "anonymous"
        count = await self.limiter.increment(client_ip)
        if count > self.limit:
            return rate_limited()
        return await call_next(request)
