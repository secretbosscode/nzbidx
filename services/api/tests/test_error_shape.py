"""Ensure error responses follow the JSON schema."""

from pathlib import Path
import sys

from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.testclient import TestClient

# Ensure package import
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.api_key import ApiKeyMiddleware  # noqa: E402
from nzbidx_api.middleware_quota import QuotaMiddleware  # noqa: E402
from nzbidx_api.errors import forbidden, breaker_open  # noqa: E402


async def ok(request):
    return JSONResponse({"ok": True})


def forbidden_route(request):
    return forbidden()


def unavailable_route(request):
    return breaker_open()


def create_app():
    routes = [
        Route("/api", ok),
        Route("/forbidden", forbidden_route),
        Route("/unavail", unavailable_route),
    ]
    middleware = [Middleware(ApiKeyMiddleware), Middleware(QuotaMiddleware)]
    return Starlette(routes=routes, middleware=middleware)


def test_error_shapes(monkeypatch):
    monkeypatch.setenv("API_KEYS", "k")
    monkeypatch.setenv("KEY_RATE_LIMIT", "1")
    monkeypatch.setenv("KEY_RATE_WINDOW", "60")
    app = create_app()
    client = TestClient(app)

    # 401
    resp = client.get("/api")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"

    # 403
    resp = client.get("/forbidden", headers={"X-Api-Key": "k"})
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden"

    # 429
    client.get("/api", headers={"X-Api-Key": "k"})
    resp = client.get("/api", headers={"X-Api-Key": "k"})
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limited"

    # 503
    resp = client.get("/unavail", headers={"X-Api-Key": "k"})
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "breaker_open"
