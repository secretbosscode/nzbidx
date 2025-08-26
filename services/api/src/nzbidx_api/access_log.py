import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


logger = logging.getLogger(__name__)


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Log basic request/response details in JSON."""

    __slots__ = ()

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        if request.url.path in ("/health", "/api/health"):
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000

        logger.info(
            "access",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round(duration_ms, 3),
            },
        )

        return response
