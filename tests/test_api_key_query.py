import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from starlette.responses import Response

from nzbidx_api.api_key import ApiKeyMiddleware  # noqa: E402


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
