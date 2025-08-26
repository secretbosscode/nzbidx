"""API key authentication middleware."""

from __future__ import annotations

import base64
import binascii
import logging
from typing import Set
from hmac import compare_digest

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import api_keys, reload_api_keys
from .errors import unauthorized


logger = logging.getLogger(__name__)


def _basic_credentials(auth: str) -> Set[str]:
    """Extract credentials from a Basic Authorization header.

    Returns an empty set when the header cannot be decoded or parsed.
    """
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode()
        username, _, password = decoded.partition(":")
        return {username, password}
    except (binascii.Error, UnicodeDecodeError):
        return set()
    except Exception:  # pragma: no cover - unexpected errors
        logger.debug("Unexpected error decoding Basic auth header", exc_info=True)
        return set()


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Very small middleware that checks for a valid ``X-Api-Key`` header.

    Keys are supplied via the ``API_KEYS`` environment variable as a
    comma-separated list.  When no keys are configured all requests are
    allowed.
    """

    def __init__(self, app, reload_keys: bool = False) -> None:
        super().__init__(app)
        self.reload_keys = reload_keys
        self.valid_keys: Set[str] = api_keys()

    async def dispatch(self, request: Request, call_next) -> Response:
        if self.reload_keys:
            reload_api_keys()
            self.valid_keys = api_keys()
        if not request.url.path.startswith("/api"):
            return await call_next(request)
        if not self.valid_keys:
            return await call_next(request)
        provided = request.headers.get("X-Api-Key")
        if not provided:
            provided = request.query_params.get("apikey")
        if not provided:
            auth = request.headers.get("Authorization")
            if auth and auth.lower().startswith("basic "):
                creds = _basic_credentials(auth)
                match = creds & self.valid_keys
                if match:
                    provided = match.pop()
        if not any(compare_digest(provided or "", valid) for valid in self.valid_keys):
            return unauthorized()
        return await call_next(request)
