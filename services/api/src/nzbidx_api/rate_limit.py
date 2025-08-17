"""Simple per-IP rate limiting middleware."""

from __future__ import annotations

import os
import time
from typing import Any, Dict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import rate_limit, rate_window
from .errors import rate_limited

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
            self.client: Any = Redis.from_url(url)
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
            try:
                current = self.client.incr(redis_key)
            except Exception:  # pragma: no cover - network failure
                # Fall back to in-memory tracking on Redis errors
                self.use_redis = False
                self.client = {}
            else:
                if current == 1:
                    try:
                        self.client.expire(redis_key, self.window)
                    except Exception:  # pragma: no cover - network failure
                        # Fall back to in-memory tracking on Redis errors
                        self.use_redis = False
                        self.client = {}
                    else:
                        return int(current)
                else:
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
        limit_val = limit if limit is not None else rate_limit()
        window_val = window if window is not None else rate_window()
        self.limiter = RateLimiter(limit_val, window_val)
        self.limit = limit_val

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = request.client.host if request.client else "anonymous"
        count = self.limiter.increment(client_ip)
        if count > self.limit:
            return rate_limited()
        return await call_next(request)
