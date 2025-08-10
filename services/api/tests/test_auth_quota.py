"""Tests for per-API-key quota middleware."""

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
from nzbidx_api.middleware_quota import QuotaMiddleware  # noqa: E402


async def ok(request):
    return JSONResponse({"ok": True})


def create_app():
    routes = [Route("/api", ok)]
    middleware = [Middleware(ApiKeyMiddleware), Middleware(QuotaMiddleware)]
    return Starlette(routes=routes, middleware=middleware)


def test_quota_enforced(monkeypatch):
    monkeypatch.setenv("API_KEYS", "a")
    monkeypatch.setenv("KEY_RATE_LIMIT", "2")
    monkeypatch.setenv("KEY_RATE_WINDOW", "60")
    app = create_app()
    client = TestClient(app)
    headers = {"X-Api-Key": "a"}
    for _ in range(2):
        assert client.get("/api", headers=headers).status_code == 200
    resp = client.get("/api", headers=headers)
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limited"


def test_quota_per_key(monkeypatch):
    monkeypatch.setenv("API_KEYS", "a,b")
    monkeypatch.setenv("KEY_RATE_LIMIT", "1")
    monkeypatch.setenv("KEY_RATE_WINDOW", "60")
    app = create_app()
    client = TestClient(app)
    assert client.get("/api", headers={"X-Api-Key": "a"}).status_code == 200
    assert client.get("/api", headers={"X-Api-Key": "b"}).status_code == 200
