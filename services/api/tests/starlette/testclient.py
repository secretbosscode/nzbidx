import asyncio
import inspect
from types import SimpleNamespace
from typing import Any


class TestClient:
    """Very small subset of Starlette's TestClient for smoke tests."""

    def __init__(self, app: Any) -> None:
        self.app = app

    def __enter__(self) -> "TestClient":
        return self

    def __exit__(
        self, exc_type, exc, tb
    ) -> None:  # pragma: no cover - no cleanup needed
        return None

    def get(self, path: str, params: dict | None = None):
        request = SimpleNamespace(query_params=params or {}, headers={}, url=None)
        for route in getattr(self.app, "routes", []):
            if getattr(route, "path", None) == path:
                resp = route.endpoint(request)
                if inspect.iscoroutine(resp):
                    resp = asyncio.run(resp)
                return resp
        return SimpleNamespace(status_code=404, body=b"", headers={})

    def post(self, path: str, json: dict | None = None, headers: dict | None = None):
        async def _json():
            return json or {}

        request = SimpleNamespace(
            query_params={}, headers=headers or {}, json=_json, url=None
        )
        for route in getattr(self.app, "routes", []):
            if getattr(route, "path", None) == path:
                resp = route.endpoint(request)
                if inspect.iscoroutine(resp):
                    resp = asyncio.run(resp)
                return resp
        return SimpleNamespace(status_code=404, body=b"", headers={})
