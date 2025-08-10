from __future__ import annotations

import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import request_id_header


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Attach a unique request id to each response."""

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        header = request_id_header()
        req_id = request.headers.get(header) or str(uuid.uuid4())
        request.state.request_id = req_id
        response = await call_next(request)
        response.headers.setdefault(header, req_id)
        return response
