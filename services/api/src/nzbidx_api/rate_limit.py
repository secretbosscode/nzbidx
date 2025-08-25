"""Simple per-IP rate limiting middleware."""

from __future__ import annotations

import time
from typing import Dict
import threading

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import settings
from .errors import rate_limited


class RateLimiter:
    """Track request counts per key within a time window using memory."""

    def __init__(self, limit: int, window: int) -> None:
        self.limit = limit
        self.window = window
        self.counts: Dict[int, Dict[str, int]] = {}
        self._lock = threading.Lock()

    async def increment(self, key: str) -> int:
        """Increment and return current count for ``key``."""
        now = int(time.time())
        bucket = now // self.window
        with self._lock:
            bucket_counts = self.counts.setdefault(bucket, {})
            bucket_counts[key] = bucket_counts.get(key, 0) + 1
            # Drop old buckets
            for old in list(self.counts.keys()):
                if old != bucket:
                    del self.counts[old]
            return bucket_counts[key]


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Apply simple IP based rate limiting."""

    def __init__(
        self, app, limit: int | None = None, window: int | None = None
    ) -> None:
        super().__init__(app)
        limit_val = limit if limit is not None else settings.rate_limit
        window_val = window if window is not None else settings.rate_window
        self.limiter = RateLimiter(limit_val, window_val)
        self.limit = limit_val

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = request.client.host if request.client else "anonymous"
        count = await self.limiter.increment(client_ip)
        if count > self.limit:
            return rate_limited()
        return await call_next(request)
