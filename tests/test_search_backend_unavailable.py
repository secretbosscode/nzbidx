import asyncio
from types import SimpleNamespace

from cachetools import TTLCache
from nzbidx_api import main as api_main, search_cache


def test_search_backend_unavailable(monkeypatch):
    search_cache._CACHE = TTLCache(
        maxsize=search_cache.settings.search_cache_max_entries,
        ttl=search_cache.settings.search_ttl_seconds,
    )
    monkeypatch.setattr(api_main, "get_engine", lambda: None)
    req = SimpleNamespace(
        query_params={"t": "search"},
        headers={"Cache-Control": "no-cache"},
    )
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 500
    assert b"search backend unavailable" in resp.body
