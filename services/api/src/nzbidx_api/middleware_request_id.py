from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import request_id_header
from .otel import set_span_attr


_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")


class _RequestIDFilter(logging.Filter):
    __slots__ = ()

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        record.request_id = _request_id_ctx.get("")
        return True


logging.getLogger().addFilter(_RequestIDFilter())


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Attach a unique request id to each response and log record."""

    __slots__ = ()

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        header = request_id_header()
        req_id = request.headers.get(header) or str(uuid.uuid4())
        request.state.request_id = req_id
        token = _request_id_ctx.set(req_id)
        set_span_attr("request_id", req_id)
        try:
            response = await call_next(request)
        finally:
            _request_id_ctx.reset(token)
        response.headers.setdefault(header, req_id)
        return response
