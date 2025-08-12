class BaseHTTPMiddleware:  # pragma: no cover - minimal base class
    def __init__(self, app, *args, **kwargs) -> None:
        self.app = app

    async def dispatch(self, request, call_next):  # pragma: no cover - not used
        return await call_next(request)
