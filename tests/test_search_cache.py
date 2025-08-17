from __future__ import annotations

import asyncio

from nzbidx_api import search_cache as sc, config


def test_cache_purges_expired_entries(monkeypatch) -> None:
    sc._CACHE.clear()
    monkeypatch.setattr(config, "search_ttl_seconds", lambda: 1)
    current = [100.0]
    monkeypatch.setattr(sc.time, "time", lambda: current[0])

    asyncio.run(sc.cache_rss("old", "<rss/>"))
    assert "old" in sc._CACHE

    current[0] = 200.0
    asyncio.run(sc.cache_rss("new", "<rss2/>"))

    assert "old" not in sc._CACHE
    assert "new" in sc._CACHE


def test_manual_purge_expired(monkeypatch) -> None:
    sc._CACHE.clear()
    monkeypatch.setattr(config, "search_ttl_seconds", lambda: 1)
    current = [100.0]
    monkeypatch.setattr(sc.time, "time", lambda: current[0])

    asyncio.run(sc.cache_rss("old", "<rss/>"))
    assert "old" in sc._CACHE

    current[0] = 200.0
    sc.purge_expired()

    assert "old" not in sc._CACHE
