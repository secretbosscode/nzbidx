"""Tests for API key authentication middleware."""

from pathlib import Path
import sys

from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.testclient import TestClient

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.api_key import ApiKeyMiddleware  # noqa: E402


async def ok(request):
    return JSONResponse({"ok": True})


def create_app():
    routes = [Route("/api", ok)]
    middleware = [Middleware(ApiKeyMiddleware)]
    return Starlette(routes=routes, middleware=middleware)


def test_missing_key(monkeypatch):
    monkeypatch.setenv("API_KEYS", "valid")
    app = create_app()
    client = TestClient(app)
    resp = client.get("/api")
    assert resp.status_code == 401
    assert resp.json() == {"detail": "unauthorized"}


def test_valid_key(monkeypatch):
    monkeypatch.setenv("API_KEYS", "valid")
    app = create_app()
    client = TestClient(app)
    resp = client.get("/api", headers={"X-Api-Key": "valid"})
    assert resp.status_code == 200
