from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

from cachetools import TTLCache
from nzbidx_api import config, search_cache
from nzbidx_api.search import _format_pubdate


def test_cache_purges_expired_entries(monkeypatch):
    t = [1000.0]

    def fake_time() -> float:
        return t[0]

    monkeypatch.setattr(search_cache.time, "monotonic", fake_time)
    monkeypatch.setattr(config.settings, "search_ttl_seconds", 1)
    search_cache._CACHE = TTLCache(
        maxsize=config.settings.search_cache_max_entries,
        ttl=config.settings.search_ttl_seconds,
        timer=fake_time,
    )

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
    monkeypatch.setattr(config.settings, "search_ttl_seconds", 1)
    search_cache._CACHE = TTLCache(
        maxsize=config.settings.search_cache_max_entries,
        ttl=config.settings.search_ttl_seconds,
        timer=fake_time,
    )

    asyncio.run(search_cache.cache_rss("old", "<rss><item>old</item></rss>"))
    assert "old" in search_cache._CACHE

    t[0] += 2

    assert asyncio.run(search_cache.get_cached_rss("new")) is None
    assert "old" not in search_cache._CACHE


def test_concurrent_cache_access(monkeypatch):
    """Concurrent readers and writers should not raise runtime errors."""
    monkeypatch.setattr(config.settings, "search_ttl_seconds", 60)
    search_cache._CACHE = TTLCache(
        maxsize=config.settings.search_cache_max_entries,
        ttl=config.settings.search_ttl_seconds,
    )

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


def test_cache_rss_cacheability(monkeypatch) -> None:
    """Only responses containing an <item> element should be cached."""
    monkeypatch.setattr(config.settings, "search_ttl_seconds", 60)
    search_cache._CACHE = TTLCache(
        maxsize=config.settings.search_cache_max_entries,
        ttl=config.settings.search_ttl_seconds,
    )

    # Non-cacheable: no <item> element
    asyncio.run(search_cache.cache_rss("empty", "<rss></rss>"))
    assert "empty" not in search_cache._CACHE

    # Cacheable: contains <item> element
    asyncio.run(search_cache.cache_rss("full", "<rss><item>1</item></rss>"))
    assert "full" in search_cache._CACHE


def test_search_logs_cache_hit(monkeypatch, caplog) -> None:
    from nzbidx_api import main as api_main

    search_cache._CACHE = TTLCache(
        maxsize=config.settings.search_cache_max_entries,
        ttl=config.settings.search_ttl_seconds,
    )
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
        for name in ("nzbidx_api.search", "nzbidx_api.search_cache"):
            logging.getLogger(name).addHandler(caplog.handler)
        resp1 = asyncio.run(api_main.api(req))
        resp2 = asyncio.run(api_main.api(req))

    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert "search_query" in caplog.text
    assert "search_cache_hit" in caplog.text
