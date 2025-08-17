from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

from starlette.responses import Response

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api.middleware_security import SecurityMiddleware  # noqa: E402
from nzbidx_api.orjson_response import orjson  # noqa: E402


async def _call_next(_request):
    return Response("ok")


def _dispatch_with_header(header_value: str):
    middleware = SecurityMiddleware(app=None, max_request_bytes=1024)
    request = SimpleNamespace(headers={"content-length": header_value})
    return asyncio.run(middleware.dispatch(request, _call_next))


def test_rejects_non_numeric_content_length():
    resp = _dispatch_with_header("not-a-number")
    assert resp.status_code == 400
    body = orjson.loads(resp.body)
    assert body["detail"] == "invalid Content-Length header"


def test_rejects_float_content_length():
    resp = _dispatch_with_header("12.34")
    assert resp.status_code == 400
    body = orjson.loads(resp.body)
    assert body["detail"] == "invalid Content-Length header"
