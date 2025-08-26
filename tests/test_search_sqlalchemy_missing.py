import asyncio
from types import SimpleNamespace

from cachetools import TTLCache
from nzbidx_api import main as api_main, search as search_mod, search_cache


def test_search_sqlalchemy_missing(monkeypatch):
    search_cache._CACHE = TTLCache(
        maxsize=search_cache.settings.search_cache_max_entries,
        ttl=search_cache.settings.search_ttl_seconds,
    )
    # Simulate SQLAlchemy not being installed
    monkeypatch.setattr(search_mod, "text", None)
    # Provide dummy engines so _search and search_releases_async pass engine checks
    dummy_engine = object()
    monkeypatch.setattr(api_main, "get_engine", lambda: dummy_engine)
    monkeypatch.setattr(search_mod, "get_engine", lambda: dummy_engine)

    req = SimpleNamespace(
        query_params={"t": "search"}, headers={"Cache-Control": "no-cache"}
    )
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 500
    assert b"search backend unavailable" in resp.body
