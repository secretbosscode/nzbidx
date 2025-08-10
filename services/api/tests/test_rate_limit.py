"""Tests for the rate limiting middleware."""

from pathlib import Path
import sys

from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.testclient import TestClient

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.rate_limit import RateLimitMiddleware  # noqa: E402


async def ok(request):
    return JSONResponse({"ok": True})


def create_app():
    routes = [Route("/ok", ok)]
    middleware = [Middleware(RateLimitMiddleware)]
    return Starlette(routes=routes, middleware=middleware)


def test_rate_limit_exceeded(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT", "2")
    monkeypatch.setenv("RATE_WINDOW", "60")
    app = create_app()
    client = TestClient(app)
    for _ in range(2):
        assert client.get("/ok").status_code == 200
    response = client.get("/ok")
    assert response.status_code == 429
    assert response.json()["error"]["code"] == "rate_limited"
