"""Per-API-key quota middleware."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .rate_limit import RateLimiter
from .config import key_rate_limit, key_rate_window
from .errors import rate_limited
from .metrics_log import inc_rate_limited


class QuotaMiddleware(BaseHTTPMiddleware):
    """Token bucket keyed by ``X-Api-Key`` header."""

    def __init__(
        self, app, limit: int | None = None, window: int | None = None
    ) -> None:
        super().__init__(app)
        limit_val = limit if limit is not None else key_rate_limit()
        window_val = window if window is not None else key_rate_window()
        self.limiter = RateLimiter(limit_val, window_val)
        self.limit = limit_val

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        path = getattr(getattr(request, "url", None), "path", "")
        if not path.startswith("/api"):
            return await call_next(request)
        api_key = request.headers.get("X-Api-Key")
        if not api_key:
            return await call_next(request)
        count = self.limiter.increment(api_key)
        if count > self.limit:
            inc_rate_limited()
            return rate_limited()
        return await call_next(request)
