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


def test_get_cached_rss_purges_expired_entries(monkeypatch):
    t = [1000.0]

    def fake_time() -> float:
        return t[0]

    monkeypatch.setattr(search_cache.time, "time", fake_time)
    search_cache._CACHE.clear()

    # Insert an expired entry manually
    search_cache._CACHE["old"] = (t[0] - 1, "<rss>old</rss>")

    # Lookup a different key to trigger purge_expired
    assert asyncio.run(search_cache.get_cached_rss("new")) is None
    assert "old" not in search_cache._CACHE


def test_concurrent_cache_access(monkeypatch):
    """Concurrent readers and writers should not raise runtime errors."""
    monkeypatch.setattr(config, "search_ttl_seconds", lambda: 60)
    search_cache._CACHE.clear()

    async def writer(i: int) -> None:
        await search_cache.cache_rss(f"k{i % 5}", f"<rss>{i}</rss>")

    async def reader(i: int) -> None:
        await search_cache.get_cached_rss(f"k{i % 5}")

    async def runner() -> None:
        tasks = []
        for i in range(50):
            tasks.append(asyncio.create_task(writer(i)))
            tasks.append(asyncio.create_task(reader(i)))
        await asyncio.gather(*tasks)

    asyncio.run(runner())
    # No matter the interleaving there should be at most 5 keys stored
    assert len(search_cache._CACHE) <= 5
