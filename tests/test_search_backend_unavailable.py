import asyncio
from types import SimpleNamespace

from nzbidx_api import main as api_main


def test_search_backend_unavailable(monkeypatch):
    monkeypatch.setattr(api_main, "get_engine", lambda: None)
    req = SimpleNamespace(query_params={"t": "search"}, headers={})
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 500
    assert b"search backend unavailable" in resp.body
