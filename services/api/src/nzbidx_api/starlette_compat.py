"""Starlette import helpers with minimal fallbacks for tests."""

from typing import Callable, Optional

try:  # pragma: no cover - import guard
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.routing import Route
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
except Exception:  # pragma: no cover - optional dependency

    class Request:  # type: ignore
        """Very small subset of Starlette's Request used for testing."""

        def __init__(self, scope: dict) -> None:
            self.query_params = scope.get("query_params", {})
            self.scope = scope

    class Route:  # type: ignore
        def __init__(
            self, path: str, endpoint: Callable, methods: Optional[list[str]] = None
        ) -> None:
            """Minimal route container used when Starlette isn't available."""
            self.path = path
            self.endpoint = endpoint
            self.methods = methods or ["GET"]

    class Middleware:  # type: ignore
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

    class CORSMiddleware:  # type: ignore
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

    class BaseHTTPMiddleware:  # type: ignore
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

    class Starlette:  # type: ignore
        def __init__(
            self,
            *,
            routes: Optional[list[Route]] = None,
            on_startup: Optional[list[Callable]] = None,
            on_shutdown: Optional[list[Callable]] = None,
            middleware: Optional[list[Middleware]] = None,
        ) -> None:
            """Store basic application configuration for tests.

            This stub only keeps track of routes so that a lightweight test
            client can dispatch to the correct endpoint.  The full ASGI
            interface and middleware handling provided by Starlette are far
            beyond the needs of the smoke tests, so they are intentionally
            omitted.
            """
            self.routes = routes or []
            self.on_startup = on_startup or []
            self.on_shutdown = on_shutdown or []
            self.middleware = middleware or []
