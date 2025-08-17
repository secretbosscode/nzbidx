from __future__ import annotations

import asyncio

from nzbidx_api import config, search_cache


def test_cache_purges_expired_entries(monkeypatch):
    t = [1000.0]

    def fake_time() -> float:
        return t[0]

    monkeypatch.setattr(search_cache.time, "time", fake_time)
    monkeypatch.setattr(config, "search_ttl_seconds", lambda: 1)
    search_cache._CACHE.clear()

    asyncio.run(search_cache.cache_rss("old", "<rss>old</rss>"))
    assert "old" in search_cache._CACHE

    t[0] += 2
    asyncio.run(search_cache.cache_rss("new", "<rss>new</rss>"))

    assert "old" not in search_cache._CACHE
    assert "new" in search_cache._CACHE
