import asyncio
import base64
from types import SimpleNamespace

from starlette.responses import Response

from nzbidx_api.api_key import ApiKeyMiddleware


async def _call_next(_request):
    return Response("ok")


def test_accepts_query_param(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    middleware = ApiKeyMiddleware(app=None)
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={},
        query_params={"apikey": "secret"},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 200


def test_rejects_invalid_query_param(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    middleware = ApiKeyMiddleware(app=None)
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={},
        query_params={"apikey": "bad"},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 401


def test_accepts_basic_password(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    middleware = ApiKeyMiddleware(app=None)
    encoded = base64.b64encode(b"user:secret").decode()
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={"Authorization": f"Basic {encoded}"},
        query_params={},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 200


def test_accepts_basic_username(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    middleware = ApiKeyMiddleware(app=None)
    encoded = base64.b64encode(b"secret:pass").decode()
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={"Authorization": f"Basic {encoded}"},
        query_params={},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 200


def test_rejects_invalid_basic(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    middleware = ApiKeyMiddleware(app=None)
    encoded = base64.b64encode(b"user:bad").decode()
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={"Authorization": f"Basic {encoded}"},
        query_params={},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 401
