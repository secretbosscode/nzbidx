"""Security related middleware."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from .orjson_response import ORJSONResponse, Response


class SecurityMiddleware(BaseHTTPMiddleware):
    """Add security headers and enforce a request size limit."""

    def __init__(self, app, max_request_bytes: int) -> None:
        super().__init__(app)
        self.max_request_bytes = max_request_bytes

    async def dispatch(self, request: Request, call_next) -> Response:
        length = request.headers.get("content-length")
        if length:
            try:
                if int(length) > self.max_request_bytes:
                    return ORJSONResponse(
                        {"detail": "request too large"}, status_code=413
                    )
            except ValueError:
                return ORJSONResponse(
                    {"detail": "invalid Content-Length header"}, status_code=400
                )
        response = await call_next(request)
        headers = response.headers
        headers.setdefault("X-Content-Type-Options", "nosniff")
        headers.setdefault("Referrer-Policy", "no-referrer")
        headers.setdefault("X-Frame-Options", "DENY")
        headers.setdefault("X-Download-Options", "noopen")
        headers.setdefault("Permissions-Policy", "interest-cohort=()")
        return response
