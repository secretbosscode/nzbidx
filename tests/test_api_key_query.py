import asyncio
import base64
from types import SimpleNamespace

from starlette.responses import Response

from nzbidx_api.api_key import ApiKeyMiddleware
from nzbidx_api.config import reload_api_keys


async def _call_next(_request):
    return Response("ok")


def test_accepts_query_param(monkeypatch):
    monkeypatch.setenv("API_KEYS", "secret")
    reload_api_keys()
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
    reload_api_keys()
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
    reload_api_keys()
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
    reload_api_keys()
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
    reload_api_keys()
    middleware = ApiKeyMiddleware(app=None)
    encoded = base64.b64encode(b"user:bad").decode()
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={"Authorization": f"Basic {encoded}"},
        query_params={},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 401


def test_runtime_key_refresh(monkeypatch):
    monkeypatch.setenv("API_KEYS", "first")
    reload_api_keys()
    middleware = ApiKeyMiddleware(app=None, reload_keys=True)

    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={},
        query_params={"apikey": "first"},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 200

    monkeypatch.setenv("API_KEYS", "second")
    request = SimpleNamespace(
        url=SimpleNamespace(path="/api/health"),
        headers={},
        query_params={"apikey": "second"},
    )
    resp = asyncio.run(middleware.dispatch(request, _call_next))
    assert resp.status_code == 200
