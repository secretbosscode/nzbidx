"""Validate querystring and parameter length limits."""

from pathlib import Path
import sys
import asyncio
import json

# Ensure importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes):
        from urllib.parse import parse_qs

        self.query_string = query_string
        self.query_params = {
            k: v[0] for k, v in parse_qs(query_string.decode()).items()
        }
        self.headers: dict[str, str] = {}


def _json(resp):
    return json.loads(resp.body)


def test_querystring_too_long(monkeypatch):
    monkeypatch.setenv("MAX_QUERY_BYTES", "10")
    request = DummyRequest(b"t=search&" + b"a" * 20)
    resp = asyncio.run(main.api(request))
    assert resp.status_code == 400
    assert _json(resp)["error"]["code"] == "invalid_params"


def test_param_too_long(monkeypatch):
    monkeypatch.setenv("MAX_QUERY_BYTES", "100")
    monkeypatch.setenv("MAX_PARAM_BYTES", "5")
    request = DummyRequest(b"t=search&q=" + b"a" * 6)
    resp = asyncio.run(main.api(request))
    assert resp.status_code == 400
    assert _json(resp)["error"]["code"] == "invalid_params"
