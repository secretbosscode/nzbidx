import asyncio
from types import SimpleNamespace
from nzbidx_api import main as api_main


def test_movie_without_query_returns_placeholder(monkeypatch):
    monkeypatch.setattr(api_main, "_search", lambda *a, **k: [])
    req = SimpleNamespace(query_params={"t": "movie"}, headers={})
    resp = asyncio.run(api_main.api(req))
    body = resp.body.decode()
    assert "Indexer Test Item" in body
