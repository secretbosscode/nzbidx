from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

from nzbidx_api import search_cache, main as api_main
from nzbidx_api.search import _format_pubdate


def test_cache_purges_expired_entries(monkeypatch):
    t = [1000.0]

    def fake_time() -> float:
        return t[0]

    monkeypatch.setattr(search_cache.time, "monotonic", fake_time)
    monkeypatch.setattr(search_cache, "search_ttl_seconds", lambda: 1)
    search_cache._CACHE.clear()

    asyncio.run(search_cache.cache_rss("old", "<rss><item>old</item></rss>"))
    assert "old" in search_cache._CACHE

    t[0] += 2
    asyncio.run(search_cache.cache_rss("new", "<rss><item>new</item></rss>"))

    assert "old" not in search_cache._CACHE
    assert "new" in search_cache._CACHE


def test_get_cached_rss_purges_expired_entries(monkeypatch):
    t = [1000.0]

    def fake_time() -> float:
        return t[0]

    monkeypatch.setattr(search_cache.time, "monotonic", fake_time)
    search_cache._CACHE.clear()

    # Insert an expired entry manually
    search_cache._CACHE["old"] = (t[0] - 1, "<rss><item>old</item></rss>")

    # Lookup a different key to trigger purge_expired
    assert asyncio.run(search_cache.get_cached_rss("new")) is None
    assert "old" not in search_cache._CACHE


def test_concurrent_cache_access(monkeypatch):
    """Concurrent readers and writers should not raise runtime errors."""
    monkeypatch.setattr(search_cache, "search_ttl_seconds", lambda: 60)
    search_cache._CACHE.clear()

    async def writer(i: int) -> None:
        await search_cache.cache_rss(f"k{i % 5}", f"<rss><item>{i}</item></rss>")

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


def test_cache_skips_empty_response(monkeypatch) -> None:
    monkeypatch.setattr(search_cache, "search_ttl_seconds", lambda: 60)
    search_cache._CACHE.clear()

    asyncio.run(search_cache.cache_rss("empty", "<rss></rss>"))
    assert "empty" not in search_cache._CACHE

    asyncio.run(search_cache.cache_rss("full", "<rss><item>1</item></rss>"))
    assert "full" in search_cache._CACHE


def test_search_logs_cache_hit(monkeypatch, caplog) -> None:
    search_cache._CACHE.clear()
    monkeypatch.setattr(api_main, "get_engine", lambda: object())

    async def fake_search_releases_async(*args, **kwargs):
        logging.getLogger("nzbidx_api.search").info("search_query")
        return [
            {
                "title": "foo",
                "guid": "1",
                "pubDate": _format_pubdate(None),
                "category": "5000",
                "link": "/link",
                "size": "1",
            }
        ]

    monkeypatch.setattr(api_main, "search_releases_async", fake_search_releases_async)
    req = SimpleNamespace(query_params={"t": "search"}, headers={})

    with caplog.at_level(logging.INFO):
        resp1 = asyncio.run(api_main.api(req))
        resp2 = asyncio.run(api_main.api(req))

    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert sum(r.message == "search_query" for r in caplog.records) == 1
    assert sum(r.message == "search_cache_hit" for r in caplog.records) == 1
