"""Integration tests exercising nzbidx's external dependencies."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ["API_KEYS"] = "secret"

# Ensure local packages are importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_api.main as main  # type: ignore  # noqa: E402
from nzbidx_common.os import OS_RELEASES_ALIAS  # type: ignore  # noqa: E402


try:  # pragma: no cover - prefer real TestClient when available
    from starlette.testclient import TestClient  # type: ignore
except ModuleNotFoundError:  # pragma: no cover

    class TestClient:
        """Very small subset of Starlette's TestClient for dependency tests."""

        def __init__(self, app: object) -> None:
            self.app = app

        def __enter__(self) -> "TestClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, path: str, params: dict | None = None):  # type: ignore[override]
            request = SimpleNamespace(query_params=params or {}, headers={}, url=None)
            for route in getattr(self.app, "routes", []):
                if getattr(route, "path", None) == path:
                    resp = route.endpoint(request)
                    if inspect.iscoroutine(resp):
                        resp = asyncio.run(resp)
                    return resp
            return SimpleNamespace(status_code=404, body=b"", headers={})

        def post(self, path: str, json: dict | None = None, headers: dict | None = None):  # type: ignore[override]
            async def _json() -> dict:
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


app = main.app


def test_health_endpoint() -> None:
    """Basic smoke test for CI to ensure app responds."""
    with TestClient(app) as client:
        response = client.get("/api/health")
        assert response.status_code == 200


def test_status_endpoint() -> None:
    """Status endpoint exposes dependency and breaker states."""
    with TestClient(app) as client:
        response = client.get("/api/status")
        assert response.status_code == 200
        data = json.loads(response.body)
        assert data["breaker"]["os"] == "closed"
        assert data["breaker"]["redis"] == "closed"


def test_takedown_deletes_release(monkeypatch) -> None:
    class DummyOS:
        def __init__(self) -> None:
            self.deleted = []

        def delete(self, *, index, id, refresh="wait_for") -> None:
            self.deleted.append((index, id, refresh))

    dummy = DummyOS()
    monkeypatch.setattr(main, "opensearch", dummy)
    with TestClient(app) as client:
        response = client.post(
            "/api/admin/takedown",
            headers={"X-Api-Key": "secret"},
            json={"id": "abc"},
        )
        assert response.status_code == 200
        assert dummy.deleted == [(OS_RELEASES_ALIAS, "abc", "wait_for")]
