from typing import Callable, List, Optional
from .routing import Route
from .middleware import Middleware


class Starlette:  # pragma: no cover - behaviour exercised via test client
    def __init__(
        self,
        *,
        routes: Optional[List[Route]] = None,
        on_startup: Optional[List[Callable]] = None,
        on_shutdown: Optional[List[Callable]] = None,
        middleware: Optional[List[Middleware]] = None,
    ) -> None:
        self.routes = routes or []
        self.on_startup = on_startup or []
        self.on_shutdown = on_shutdown or []
        self.middleware = middleware or []
