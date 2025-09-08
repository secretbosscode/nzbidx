"""Simple per-IP rate limiting middleware."""

from __future__ import annotations

import time
import asyncio
import ipaddress
from typing import Dict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import settings
from .errors import rate_limited


class RateLimiter:
    """Track request counts per key within a time window using memory."""

    __slots__ = ("limit", "window", "counts", "_lock")

    def __init__(self, limit: int, window: int) -> None:
        self.limit = limit
        self.window = window
        self.counts: tuple[int | None, Dict[str, int]] = (None, {})
        self._lock = asyncio.Lock()

    async def increment(self, key: str) -> int:
        """Increment and return current count for ``key``."""
        bucket = int(time.monotonic() // self.window)
        async with self._lock:
            current_bucket, bucket_counts = self.counts
            if bucket != current_bucket:
                bucket_counts = {}
                current_bucket = bucket
            bucket_counts[key] = bucket_counts.get(key, 0) + 1
            self.counts = (current_bucket, bucket_counts)
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
        self.trust_proxy_headers = settings.trust_proxy_headers

    def _trusted_ip_from_headers(self, request: Request) -> str | None:
        """Return the first valid IP from proxy headers if present."""
        for header_name in ("x-forwarded-for", "x-real-ip"):
            header = request.headers.get(header_name)
            if not header:
                continue
            for part in header.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    ipaddress.ip_address(part)
                except ValueError:
                    continue
                return part
        return None

    async def dispatch(self, request: Request, call_next) -> Response:
        if self.trust_proxy_headers:
            client_ip = self._trusted_ip_from_headers(request)
            if client_ip is None:
                client_ip = request.client.host if request.client else "anonymous"
        else:
            client_ip = request.client.host if request.client else "anonymous"
        count = await self.limiter.increment(client_ip)
        if count > self.limit:
            return rate_limited()
        return await call_next(request)
