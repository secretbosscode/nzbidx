"""API key authentication middleware."""

from __future__ import annotations

from typing import Set

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import api_keys
from .errors import unauthorized


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Very small middleware that checks for a valid ``X-Api-Key`` header.

    Keys are supplied via the ``API_KEYS`` environment variable as a
    comma-separated list.  When no keys are configured all requests are
    allowed.
    """

    def __init__(self, app) -> None:
        super().__init__(app)
        self.valid_keys: Set[str] = api_keys()

    async def dispatch(self, request: Request, call_next) -> Response:
        path = ""
        if hasattr(request, "url"):
            path = getattr(request.url, "path", "")
        if not path.startswith("/api"):
            return await call_next(request)
        if not self.valid_keys:
            return await call_next(request)
        provided = request.headers.get("X-Api-Key")
        if provided not in self.valid_keys:
            return unauthorized()
        return await call_next(request)
