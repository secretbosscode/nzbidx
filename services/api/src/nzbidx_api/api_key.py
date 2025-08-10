"""API key authentication middleware."""

from __future__ import annotations

import os
from typing import Set

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Very small middleware that checks for a valid ``X-Api-Key`` header.

    Keys are supplied via the ``API_KEYS`` environment variable as a
    comma-separated list.  When no keys are configured all requests are
    allowed.
    """

    def __init__(self, app) -> None:
        super().__init__(app)
        keys = os.getenv("API_KEYS")
        self.valid_keys: Set[str] = (
            {k.strip() for k in keys.split(",") if k.strip()} if keys else set()
        )

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self.valid_keys:
            return await call_next(request)
        provided = request.headers.get("X-Api-Key")
        if provided not in self.valid_keys:
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return await call_next(request)
