"""Tests for caching RSS responses."""

from __future__ import annotations

import asyncio

import pytest

from nzbidx_api import search_cache


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Provide a fresh cache for each test."""

    search_cache._CACHE = search_cache._new_cache()


def test_cache_rss_stores_xml_when_item_present() -> None:
    xml = "<rss><item>1</item></rss>"

    asyncio.run(search_cache.cache_rss("k", xml))

    assert asyncio.run(search_cache.get_cached_rss("k")) == xml.encode()


def test_cache_rss_skips_when_no_items() -> None:
    asyncio.run(search_cache.cache_rss("k", "<rss></rss>"))

    assert asyncio.run(search_cache.get_cached_rss("k")) is None
