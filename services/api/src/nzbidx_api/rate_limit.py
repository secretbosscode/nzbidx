"""Simple per-IP rate limiting middleware."""

from __future__ import annotations

import os
import time
from typing import Dict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

try:  # pragma: no cover - optional dependency
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore


class RateLimiter:
    """Track request counts per key within a time window."""

    def __init__(self, limit: int, window: int) -> None:
        self.limit = limit
        self.window = window
        url = os.getenv("REDIS_URL")
        if Redis and url:
            self.client = Redis.from_url(url)
            self.use_redis = True
        else:
            self.client: Dict[int, Dict[str, int]] = {}
            self.use_redis = False

    def increment(self, key: str) -> int:
        """Increment and return current count for ``key``."""
        now = int(time.time())
        bucket = now // self.window
        if self.use_redis:  # pragma: no cover - requires redis server
            redis_key = f"rl:{bucket}:{key}"
            current = self.client.incr(redis_key)
            if current == 1:
                self.client.expire(redis_key, self.window)
            return int(current)
        counts = self.client.setdefault(bucket, {})
        counts[key] = counts.get(key, 0) + 1
        for old in list(self.client.keys()):
            if old != bucket:
                del self.client[old]
        return counts[key]


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Apply simple IP based rate limiting."""

    def __init__(
        self, app, limit: int | None = None, window: int | None = None
    ) -> None:
        super().__init__(app)
        limit_val = limit if limit is not None else int(os.getenv("RATE_LIMIT", "60"))
        window_val = (
            window if window is not None else int(os.getenv("RATE_WINDOW", "60"))
        )
        self.limiter = RateLimiter(limit_val, window_val)
        self.limit = limit_val

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = request.client.host if request.client else "anonymous"
        count = self.limiter.increment(client_ip)
        if count > self.limit:
            return JSONResponse({"error": "rate limit exceeded"}, status_code=429)
        return await call_next(request)
