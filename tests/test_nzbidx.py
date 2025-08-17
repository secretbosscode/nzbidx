"""Consolidated tests for core nzbidx functionality."""

from __future__ import annotations

import importlib
import asyncio
import json
import logging
import threading
import time
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

# ruff: noqa: E402 - path manipulation before imports

# Ensure local packages are importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import nzb_builder, newznab, search as search_mod  # type: ignore
import nzbidx_api.main as api_main  # type: ignore
import nzbidx_ingest.main as main  # type: ignore
from nzbidx_ingest.main import (
    CATEGORY_MAP,
    _infer_category,
    connect_db,
    bulk_index_releases,
    OS_RELEASES_ALIAS,
)  # type: ignore


class DummyCache:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def get(self, key: str) -> bytes | None:  # type: ignore[override]
        return self.store.get(key)

    def setex(self, key: str, _ttl: int, value: bytes | str) -> None:  # type: ignore[override]
        if isinstance(value, str):
            value = value.encode("utf-8")
        self.store[key] = value


class DummyAsyncCache(DummyCache):
    async def get(self, key: str) -> bytes | None:  # type: ignore[override]
        return self.store.get(key)

    async def setex(self, key: str, _ttl: int, value: bytes | str) -> None:  # type: ignore[override]
        if isinstance(value, str):
            value = value.encode("utf-8")
        self.store[key] = value


def test_build_nzb_without_host(monkeypatch) -> None:
    monkeypatch.delenv("NNTP_HOST", raising=False)
    monkeypatch.setattr(nzb_builder, "_segments_from_db", lambda _rid: [])
    with pytest.raises(newznab.NzbFetchError):
        nzb_builder.build_nzb_for_release("MyRelease")


def test_nzb_timeout_defaults(monkeypatch) -> None:
    from nzbidx_api import config as api_config

    monkeypatch.delenv("NZB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("NNTP_TOTAL_TIMEOUT", raising=False)
    api_config.nzb_timeout_seconds.cache_clear()
    assert api_config.nzb_timeout_seconds() == 600


def test_nzb_timeout_uses_nntp_total(monkeypatch) -> None:
    from nzbidx_api import config as api_config

    monkeypatch.delenv("NZB_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("NNTP_TOTAL_TIMEOUT", "90")
    api_config.nzb_timeout_seconds.cache_clear()
    assert api_config.nzb_timeout_seconds() == 90


def test_nzb_timeout_clamped(monkeypatch) -> None:
    from nzbidx_api import config as api_config

    monkeypatch.setenv("NNTP_TOTAL_TIMEOUT", "50")
    monkeypatch.setenv("NZB_TIMEOUT_SECONDS", "10")
    api_config.nzb_timeout_seconds.cache_clear()
    assert api_config.nzb_timeout_seconds() == 50


def test_build_nzb_clears_nzb_timeout_cache(monkeypatch) -> None:
    from nzbidx_api import config as api_config
    from nzbidx_api import nzb_builder

    monkeypatch.setenv("NZB_TIMEOUT_SECONDS", "10")
    monkeypatch.setenv("NNTP_TOTAL_TIMEOUT", "10")
    api_config.nzb_timeout_seconds.cache_clear()
    assert api_config.nzb_timeout_seconds() == 10

    monkeypatch.setenv("NZB_TIMEOUT_SECONDS", "20")
    assert api_config.nzb_timeout_seconds() == 10

    monkeypatch.setattr(
        nzb_builder, "_segments_from_db", lambda _rid: [(1, "m1", "g", 123)]
    )

    nzb_builder.build_nzb_for_release("MyRelease")

    assert api_config.nzb_timeout_seconds() == 20


def test_build_nzb_missing_segments_raises(monkeypatch) -> None:
    """Builder should raise when no DB segments exist."""

    monkeypatch.setattr(nzb_builder, "_segments_from_db", lambda _rid: [])
    with pytest.raises(newznab.NzbFetchError):
        nzb_builder.build_nzb_for_release("MyRelease")


def test_release_not_found_logs(monkeypatch, caplog) -> None:
    """Missing release should emit a specific warning."""

    class DummyCursor:
        def execute(self, *args, **kwargs):
            pass

        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def close(self):
            pass

    DummyConn.__module__ = "sqlite3"

    def _connect() -> DummyConn:
        return DummyConn()

    monkeypatch.setattr(main, "connect_db", _connect)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(newznab.NzbFetchError, match="release not found"):
            nzb_builder.build_nzb_for_release("missing")

    assert any(
        rec.message == "release_not_found" and rec.release_id == "missing"
        for rec in caplog.records
    )


def test_missing_segments_logs(monkeypatch, caplog) -> None:
    """Releases without segments should emit a specific warning."""

    class DummyCursor:
        def execute(self, *args, **kwargs):
            pass

        def fetchone(self):
            return (None,)

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def close(self):
            pass

    DummyConn.__module__ = "sqlite3"

    def _connect() -> DummyConn:
        return DummyConn()

    monkeypatch.setattr(main, "connect_db", _connect)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(newznab.NzbFetchError, match="release has no segments"):
            nzb_builder.build_nzb_for_release("noparts")

    assert any(
        rec.message == "missing_segments" and rec.release_id == "noparts"
        for rec in caplog.records
    )


def test_lookup_error_missing_segments_suggests_backfill(monkeypatch) -> None:
    """Missing segments should suggest running the backfill script."""

    def _missing(_rid: str):
        raise LookupError("release has no segments")

    monkeypatch.setattr(nzb_builder, "_segments_from_db", _missing)
    with pytest.raises(newznab.NzbFetchError) as excinfo:
        nzb_builder.build_nzb_for_release("missing")
    msg = str(excinfo.value)
    assert "scripts/backfill_release_parts.py" in msg
    assert "release has no segments" in msg


def test_lookup_error_not_found_mentions_normalisation(monkeypatch) -> None:
    """Not found errors explain normalisation."""

    def _missing(_rid: str):
        raise LookupError("release not found")

    monkeypatch.setattr(nzb_builder, "_segments_from_db", _missing)
    with pytest.raises(newznab.NzbFetchError) as excinfo:
        nzb_builder.build_nzb_for_release("missing")
    msg = str(excinfo.value)
    assert "release not found" in msg
    assert "scripts/backfill_release_parts.py" not in msg
    assert "release ID is normalized" in msg


def test_db_query_failure_logs(monkeypatch, caplog) -> None:
    """Database errors should be logged and wrapped."""

    class DummyCursor:
        def execute(self, *args, **kwargs):
            raise RuntimeError("boom")

        def fetchone(self):
            return (1,)

        def fetchall(self):
            return []

    class DummyConn:
        def cursor(self):
            return DummyCursor()

        def close(self):
            pass

    DummyConn.__module__ = "sqlite3"

    def _connect() -> DummyConn:
        return DummyConn()

    monkeypatch.setattr(main, "connect_db", _connect)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(newznab.NzbDatabaseError, match="boom"):
            nzb_builder.build_nzb_for_release("broken")

    assert any(
        rec.message == "db_query_failed"
        and rec.release_id == "broken"
        and rec.exception == "RuntimeError"
        for rec in caplog.records
    )


def test_builds_nzb_from_db(monkeypatch) -> None:
    """Segments fetched from the DB should generate an NZB."""

    monkeypatch.setattr(
        nzb_builder,
        "_segments_from_db",
        lambda _rid: [
            (1, "msg1@example.com", "g", 123),
            (2, "<msg2@example.com>", "g", 456),
        ],
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert '<segment bytes="123" number="1">msg1@example.com</segment>' in xml
    assert '<segment bytes="456" number="2">msg2@example.com</segment>' in xml


def test_segment_limit_exceeded(monkeypatch, caplog) -> None:
    """Exceeding the segment limit should raise an error."""

    monkeypatch.setenv("NZB_MAX_SEGMENTS", "5")
    from nzbidx_api import config as api_config

    api_config.nzb_max_segments.cache_clear()
    segs = [(i, f"msg{i}@example.com", "g", 0) for i in range(1, 11)]
    monkeypatch.setattr(nzb_builder, "_segments_from_db", lambda _rid: segs)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(newznab.NzbFetchError):
            nzb_builder.build_nzb_for_release("MyRelease")
    assert any(rec.message == "segment_limit_exceeded" for rec in caplog.records)


def test_basic_api_and_ingest(monkeypatch) -> None:
    """Ensure search sort and ingest category inference work."""
    # Ingest: category inference
    assert _infer_category("Awesome Film [movies]") == CATEGORY_MAP["movies"]

    # API: verify search applies sort
    body_holder: dict[str, object] = {}

    class DummyClient:
        def search(self, **kwargs):
            body_holder["body"] = kwargs["body"]
            return {"hits": {"hits": []}}

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    search_mod.search_releases(client, {"must": []}, limit=5, sort="date")

    assert body_holder["body"]["sort"] == [{"posted_at": {"order": "desc"}}]


def test_search_negative_limit() -> None:
    """Negative ``limit`` values should raise ``ValueError``."""

    with pytest.raises(ValueError, match="limit must be >= 0"):
        search_mod.search_releases(object(), {"must": []}, limit=-1)


def test_search_negative_offset() -> None:
    """Negative ``offset`` values should raise ``ValueError``."""

    with pytest.raises(ValueError, match="offset must be >= 0"):
        search_mod.search_releases(object(), {"must": []}, limit=1, offset=-1)


def test_bulk_index_releases_builds_payload() -> None:
    """Bulk payload should include action and source pairs."""

    captured: dict[str, object] = {}

    class DummyClient:
        def bulk(self, *, body: str, refresh: bool) -> None:
            captured["body"] = body
            captured["refresh"] = refresh

    docs = [
        ("id1", {"norm_title": "one", "category": "2000"}),
        ("id2", {"norm_title": "two", "category": "3000"}),
    ]

    bulk_index_releases(DummyClient(), docs)

    lines = captured["body"].splitlines()
    assert json.loads(lines[0]) == {
        "index": {"_index": OS_RELEASES_ALIAS, "_id": "id1"}
    }
    assert json.loads(lines[1])["norm_title"] == "one"


def test_bulk_index_releases_deletes() -> None:
    """Passing ``None`` deletes documents via the bulk API."""

    captured: dict[str, object] = {}

    class DummyClient:
        def bulk(self, *, body: str, refresh: bool) -> None:  # type: ignore[override]
            captured["body"] = body

    bulk_index_releases(DummyClient(), [("id1", None)])

    lines = captured["body"].splitlines()
    assert lines and json.loads(lines[0]) == {
        "delete": {"_index": OS_RELEASES_ALIAS, "_id": "id1"}
    }
    assert len(lines) == 1


def test_bulk_index_releases_logs_errors(caplog) -> None:
    """Bulk API errors should emit a warning for each failed item."""

    class DummyClient:
        def bulk(self, *, body: str, refresh: bool) -> dict[str, object]:  # type: ignore[override]
            return {
                "errors": True,
                "items": [
                    {"index": {"_id": "id1", "error": {"reason": "boom"}}},
                    {"index": {"_id": "id2"}},
                ],
            }

    docs = [
        ("id1", {"norm_title": "one", "category": "2000"}),
        ("id2", {"norm_title": "two", "category": "3000"}),
    ]

    with caplog.at_level(logging.WARNING):
        bulk_index_releases(DummyClient(), docs)

    assert any(
        rec.message == "opensearch_bulk_item_failed"
        and rec.id == "id1"
        and rec.error == "boom"
        for rec in caplog.records
    )


def test_os_search_multiple_categories(monkeypatch) -> None:
    """Multiple categories should yield a ``terms`` filter."""
    captured: dict[str, object] = {}

    def dummy_search(_client, query, *, limit, offset=0, sort=None, api_key=None):
        captured["query"] = query
        return []

    monkeypatch.setattr(api_main, "search_releases", dummy_search)
    monkeypatch.setattr(api_main, "opensearch", object())
    api_main._os_search("test", category="1000,2000")

    filters = captured["query"].get("filter", [])
    assert {"terms": {"category": ["1000", "2000"]}} in filters


def test_os_search_without_query(monkeypatch) -> None:
    """Searches without parameters should fall back to ``match_all``."""
    captured: dict[str, object] = {}

    def dummy_search(_client, query, *, limit, offset=0, sort=None, api_key=None):
        captured["query"] = query
        return []

    monkeypatch.setattr(api_main, "search_releases", dummy_search)
    monkeypatch.setattr(api_main, "opensearch", object())
    api_main._os_search(None, category="2040")
    assert {"match_all": {}} in captured["query"].get("must", [])


def test_os_search_ignores_whitespace_query(monkeypatch) -> None:
    """Whitespace-only queries should also trigger ``match_all``."""
    captured: dict[str, object] = {}

    def dummy_search(_client, query, *, limit, offset=0, sort=None, api_key=None):
        captured["query"] = query
        return []

    monkeypatch.setattr(api_main, "search_releases", dummy_search)
    monkeypatch.setattr(api_main, "opensearch", object())
    api_main._os_search("   ", category="2040")
    assert {"match_all": {}} in captured["query"].get("must", [])


def test_movie_defaults_to_all_movie_categories(monkeypatch) -> None:
    """Movie searches should include all movie subcategories by default."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "movie"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.MOVIE_CATEGORY_IDS


def test_movie_respects_cat_param(monkeypatch) -> None:
    """Explicit category filters should be honored."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "movie", "cat": "2030"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"] == "2030"


def test_tv_defaults_to_all_tv_categories(monkeypatch) -> None:
    """TV searches should include all TV subcategories by default."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "tvsearch"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.TV_CATEGORY_IDS


def test_tv_respects_cat_param(monkeypatch) -> None:
    """Explicit TV category filters should be honored."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "tvsearch", "cat": "5030"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"] == "5030"


def test_music_defaults_to_all_audio_categories(monkeypatch) -> None:
    """Music searches should include all audio subcategories by default."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "music"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.AUDIO_CATEGORY_IDS


def test_music_respects_cat_param(monkeypatch) -> None:
    """Explicit music category filters should be honored."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "music", "cat": "3030"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"] == "3030"


def test_book_defaults_to_all_book_categories(monkeypatch) -> None:
    """Book searches should include all book subcategories by default."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "book"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.BOOKS_CATEGORY_IDS


def test_book_respects_cat_param(monkeypatch) -> None:
    """Explicit book category filters should be honored."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(query_params={"t": "book", "cat": "7030"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"] == "7030"


def test_parent_cat_expands_subcategories(monkeypatch) -> None:
    """Parent category IDs should expand to include subcategories."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(
        query_params={"t": "search", "q": "test", "cat": "5000"},
        headers={},
    )
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.TV_CATEGORY_IDS


def test_adult_parent_cat_expands(monkeypatch) -> None:
    """Adult parent category should expand when adult content allowed."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    req = SimpleNamespace(
        query_params={"t": "search", "q": "test", "cat": "6000"},
        headers={},
    )
    asyncio.run(api_main.api(req))
    assert captured["category"].split(",") == newznab.ADULT_CATEGORY_IDS


def test_strips_adult_cats_when_disallowed(monkeypatch) -> None:
    """Adult categories should be removed when not allowed."""

    captured: dict[str, object] = {}

    def dummy_os_search(
        q,
        *,
        category,
        tag=None,
        extra=None,
        limit=50,
        offset=0,
        sort=None,
        api_key=None,
    ):
        captured["category"] = category
        return []

    monkeypatch.setattr(api_main, "_os_search", dummy_os_search)
    monkeypatch.setenv("ALLOW_XXX", "false")
    req = SimpleNamespace(query_params={"t": "movie", "cat": "2030,6010"}, headers={})
    asyncio.run(api_main.api(req))
    assert captured["category"] == "2030"


def test_getnzb_timeout(monkeypatch) -> None:
    """Slow NZB generation should return 504 after timeout."""

    async def slow_get_nzb(_release_id, _cache):
        await asyncio.sleep(0.1)
        return "<nzb></nzb>"

    monkeypatch.setattr(api_main, "get_nzb", slow_get_nzb)
    monkeypatch.setattr(api_main, "nzb_timeout_seconds", lambda: 0.01)
    cache = DummyAsyncCache()
    monkeypatch.setattr(api_main, "cache", cache)
    req = SimpleNamespace(query_params={"t": "getnzb", "id": "1"}, headers={})
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 504
    assert resp.headers["Retry-After"] == str(api_main.newznab.FAIL_TTL)
    assert "nzb:1" not in cache.store


def test_getnzb_fetch_error_returns_404(monkeypatch) -> None:
    """Fetch failures should return 404 when NZB is unavailable."""

    async def error_get_nzb(_release_id, _cache):
        raise newznab.NzbFetchError("boom")

    monkeypatch.setattr(api_main, "get_nzb", error_get_nzb)
    req = SimpleNamespace(query_params={"t": "getnzb", "id": "1"}, headers={})
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 404
    assert "Retry-After" not in resp.headers
    assert json.loads(resp.body) == {
        "error": {
            "code": "nzb_not_found",
            "message": "No segments found for release 1",
        }
    }


def test_getnzb_database_error_returns_503(monkeypatch) -> None:
    """Database errors should return 503 and not be cached."""

    def db_error_build(_release_id: str) -> str:
        raise newznab.NzbDatabaseError("db down")

    monkeypatch.setattr(
        newznab.nzb_builder, "build_nzb_for_release", db_error_build
    )
    cache = DummyAsyncCache()
    monkeypatch.setattr(api_main, "cache", cache)
    req = SimpleNamespace(query_params={"t": "getnzb", "id": "1"}, headers={})
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 503
    assert json.loads(resp.body) == {
        "error": {"code": "nzb_unavailable", "message": "database query failed"}
    }
    assert "nzb:1" not in cache.store


def test_getnzb_sets_content_disposition(monkeypatch) -> None:
    """NZB downloads should include a content-disposition header."""

    async def fake_get_nzb(_release_id, _cache):
        return "<nzb></nzb>"

    monkeypatch.setattr(api_main, "get_nzb", fake_get_nzb)
    req = SimpleNamespace(query_params={"t": "getnzb", "id": "123"}, headers={})
    resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 200
    assert resp.headers["Content-Disposition"] == 'attachment; filename="123.nzb"'
    assert resp.headers["content-type"] == "application/x-nzb"


def test_caps_xml_omits_adult_when_disabled(monkeypatch) -> None:
    """caps.xml should exclude adult categories when disabled."""

    monkeypatch.setenv("SAFESEARCH", "on")
    reloaded = importlib.reload(newznab)
    xml = reloaded.caps_xml()
    assert '<category id="6000"' not in xml


def test_infer_category_from_group() -> None:
    """Group names should hint at the correct category."""
    assert (
        _infer_category("Test", group="alt.binaries.psp") == CATEGORY_MAP["console_psp"]
    )
    assert (
        _infer_category("Test", group="alt.binaries.pc.games")
        == CATEGORY_MAP["pc_games"]
    )


def test_caps_xml_uses_config(tmp_path, monkeypatch) -> None:
    """caps.xml should reflect configured categories."""
    cfg = tmp_path / "cats.json"
    cfg.write_text(
        json.dumps(
            [
                {"id": 123, "name": "Foo"},
                {"id": 6000, "name": "Adult"},
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CATEGORY_CONFIG", str(cfg))
    reloaded = importlib.reload(newznab)
    xml = reloaded.caps_xml()
    assert '<category id="123" name="Foo"/>' in xml
    assert '<category id="6000"' in xml


def test_caps_xml_defaults(monkeypatch) -> None:
    """caps.xml should include all predefined categories by default."""
    monkeypatch.delenv("CATEGORY_CONFIG", raising=False)
    reloaded = importlib.reload(newznab)
    xml = reloaded.caps_xml()
    assert '<category id="1000" name="Console"/>' in xml
    assert '<category id="7030" name="Comics"/>' in xml
    assert '<category id="6090" name="XXX/WEB-DL"/>' in xml


@pytest.mark.parametrize("cache_cls", [DummyCache, DummyAsyncCache])
def test_failed_fetch_not_cached(monkeypatch, cache_cls) -> None:
    cache = cache_cls()
    calls: list[str] = []

    def boom(release_id: str) -> str:
        calls.append(release_id)
        raise RuntimeError("boom")

    monkeypatch.setattr(newznab.nzb_builder, "build_nzb_for_release", boom)

    key = "nzb:123"
    # first call should surface the error and not cache a sentinel
    with pytest.raises(RuntimeError):
        asyncio.run(newznab.get_nzb("123", cache))
    assert key not in cache.store
    assert calls == ["123"]

    calls.clear()
    # second call should invoke builder again
    with pytest.raises(RuntimeError):
        asyncio.run(newznab.get_nzb("123", cache))
    assert calls == ["123"]


@pytest.mark.parametrize("cache_cls", [DummyCache, DummyAsyncCache])
def test_database_error_not_cached(monkeypatch, cache_cls) -> None:
    cache = cache_cls()

    def db_error(_release_id: str) -> str:
        raise newznab.NzbDatabaseError("db down")

    monkeypatch.setattr(newznab.nzb_builder, "build_nzb_for_release", db_error)

    key = "nzb:123"
    with pytest.raises(newznab.NzbDatabaseError):
        asyncio.run(newznab.get_nzb("123", cache))
    assert key not in cache.store


@pytest.mark.parametrize("cache_cls", [DummyCache, DummyAsyncCache])
def test_cached_nzb_served(monkeypatch, cache_cls) -> None:
    """A cached NZB should be returned without rebuilding."""

    cache = cache_cls()
    init_calls: list[int] = []

    async def fake_init_cache_async() -> None:
        init_calls.append(1)
        api_main.cache = cache

    monkeypatch.setattr(api_main, "init_cache_async", fake_init_cache_async)
    api_main.cache = None

    build_calls: list[str] = []

    def fake_build(release_id: str) -> str:
        build_calls.append(release_id)
        return "<nzb></nzb>"

    monkeypatch.setattr(newznab.nzb_builder, "build_nzb_for_release", fake_build)

    req = SimpleNamespace(query_params={"t": "getnzb", "id": "123"}, headers={})
    resp1 = asyncio.run(api_main.api(req))
    assert resp1.status_code == 200
    assert build_calls == ["123"]

    build_calls.clear()
    resp2 = asyncio.run(api_main.api(req))
    assert resp2.status_code == 200
    assert build_calls == []
    assert len(init_calls) == 1


def test_connect_db_creates_parent(tmp_path, monkeypatch) -> None:
    db_file = tmp_path / "sub" / "test.db"
    monkeypatch.setenv("DATABASE_URL", str(db_file))
    conn = connect_db()
    conn.execute("SELECT 1")
    conn.close()
    assert db_file.exists()


def test_connect_db_postgres(monkeypatch) -> None:
    calls: dict[str, str] = {}

    class DummyConn:
        def execute(self, stmt: str) -> None:  # pragma: no cover - trivial
            calls["stmt"] = stmt

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def rollback(self) -> None:  # pragma: no cover - trivial
            return None

        def __enter__(self) -> "DummyConn":  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
            return None

    class DummyEngine:
        def connect(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def begin(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def raw_connection(self):  # pragma: no cover - trivial
            class Raw:
                def cursor(self):  # pragma: no cover - trivial
                    class C:
                        rowcount = 0

                        def execute(self, *a, **k) -> None:
                            return None

                    return C()

                def commit(self) -> None:  # pragma: no cover - trivial
                    return None

            return Raw()

    def fake_create_engine(
        url: str, echo: bool = False, future: bool = True
    ) -> DummyEngine:
        calls["url"] = url
        return DummyEngine()

    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")
    monkeypatch.setattr(main, "create_engine", fake_create_engine)
    monkeypatch.setattr(main, "text", lambda s: s)
    conn = connect_db()
    assert calls["url"] == "postgresql+psycopg://user@host/db"
    assert hasattr(conn, "cursor")


def test_connect_db_postgres_single_slash(monkeypatch) -> None:
    calls: dict[str, str] = {}

    class DummyConn:
        def execute(self, stmt: str) -> None:  # pragma: no cover - trivial
            calls["stmt"] = stmt

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def rollback(self) -> None:  # pragma: no cover - trivial
            return None

        def __enter__(self) -> "DummyConn":  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
            return None

    class DummyEngine:
        def connect(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def begin(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def raw_connection(self):  # pragma: no cover - trivial
            class Raw:
                def cursor(self):  # pragma: no cover - trivial
                    class C:
                        rowcount = 0

                        def execute(self, *a, **k) -> None:
                            return None

                    return C()

                def commit(self) -> None:  # pragma: no cover - trivial
                    return None

            return Raw()

    def fake_create_engine(
        url: str, echo: bool = False, future: bool = True
    ) -> DummyEngine:
        calls["url"] = url
        return DummyEngine()

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg:/user@host/db")
    monkeypatch.setattr(main, "create_engine", fake_create_engine)
    monkeypatch.setattr(main, "text", lambda s: s)
    conn = connect_db()
    assert calls["url"] == "postgresql+psycopg://user@host/db"
    assert hasattr(conn, "cursor")


def test_connect_db_creates_database(monkeypatch) -> None:
    calls: list[str] = []
    executed: list[str] = []

    class DummyConn:
        def execute(self, stmt: str) -> None:  # pragma: no cover - trivial
            executed.append(stmt)

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def rollback(self) -> None:  # pragma: no cover - trivial
            return None

        def __enter__(self) -> "DummyConn":  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
            return None

    class Raw:
        def cursor(self):  # pragma: no cover - trivial
            class C:
                rowcount = 0

                def execute(self, *a, **k) -> None:
                    return None

            return C()

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

    class DummyEngine:
        def connect(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def begin(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def raw_connection(self):  # pragma: no cover - trivial
            return Raw()

        def dispose(self) -> None:  # pragma: no cover - trivial
            return None

    state = {"fail": True}

    def fake_create_engine(url: str, echo: bool = False, future: bool = True):
        calls.append(url)
        if state["fail"]:
            state["fail"] = False
            err = Exception("database does not exist")
            err.orig = Exception("database does not exist")
            raise err
        return DummyEngine()

    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")
    monkeypatch.setattr(main, "create_engine", fake_create_engine)
    monkeypatch.setattr(main, "text", lambda s: s)
    conn = connect_db()
    assert calls == [
        "postgresql+psycopg://user@host/db",
        "postgresql+psycopg://user@host/postgres",
        "postgresql+psycopg://user@host/db",
    ]
    assert any(stmt.startswith("CREATE DATABASE") for stmt in executed)
    assert hasattr(conn, "cursor")


def test_connect_db_missing_driver_raises(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")

    def fake_create_engine(url: str, echo: bool = False, future: bool = True):
        raise ModuleNotFoundError("No module named 'psycopg'")

    monkeypatch.setattr(main, "create_engine", fake_create_engine)
    monkeypatch.setattr(main, "text", lambda s: s)

    with pytest.raises(RuntimeError, match="psycopg"):
        connect_db()


def test_connect_db_missing_env_raises(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError):
        connect_db()


def test_nntp_client_uses_single_host_env(monkeypatch) -> None:
    monkeypatch.delenv("NNTP_HOST_1", raising=False)
    monkeypatch.setenv("NNTP_HOST", "example.org")

    import nzbidx_ingest.nntp_client as nntp_client

    called: dict[str, object] = {}

    class DummyServer:
        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            called["args"] = (host, port, user, password, timeout)

        def reader(self) -> None:  # pragma: no cover - trivial
            called["reader"] = True

        def quit(self) -> None:  # pragma: no cover - trivial
            pass

    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    client.connect()

    assert called["args"] == ("example.org", 119, None, None, 30.0)
    assert called.get("reader")


def test_nntp_client_xover(monkeypatch) -> None:
    """NNTPClient.xover should return overview data from the server."""
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")

    import nzbidx_ingest.nntp_client as nntp_client

    called: dict[str, object] = {}

    class DummyServer:
        def __init__(
            self, host, port=119, user=None, password=None, timeout=None
        ):  # pragma: no cover - trivial
            called["args"] = (host, port, user, password, timeout)

        def __enter__(self):  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            return None

        def group(self, group):  # pragma: no cover - simple
            called["group"] = group
            return "", 0, "1", "2", group

        def xover(self, start, end, *, file=None):  # pragma: no cover - simple
            called["range"] = (start, end)
            return "", [
                {"subject": "Example", "message-id": "<1@test>"},
            ]

    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    headers = client.xover("alt.binaries.example", 1, 2)

    assert headers and headers[0]["message-id"] == "<1@test>"
    assert called["args"][0] == "example.com"
    assert called["group"] == "alt.binaries.example"


def test_run_forever_respects_stop(monkeypatch) -> None:
    """run_forever should exit when the stop event is set."""
    import nzbidx_ingest.ingest_loop as loop

    calls = []

    def fake_run_once():
        calls.append(True)
        return 1

    monkeypatch.setattr(loop, "run_once", fake_run_once)

    stop = threading.Event()
    t = threading.Thread(target=loop.run_forever, args=(stop,))
    t.start()
    time.sleep(0.1)
    stop.set()
    t.join(1)
    assert calls


def test_irrelevant_groups_skipped(tmp_path, monkeypatch, caplog) -> None:
    """Groups marked irrelevant should not be polled again."""
    import nzbidx_ingest.ingest_loop as loop
    from nzbidx_ingest import config, cursors

    db_path = tmp_path / "cursors.sqlite"
    monkeypatch.setattr(config, "CURSOR_DB", str(db_path))
    monkeypatch.setattr(cursors, "CURSOR_DB", str(db_path))

    cursors.mark_irrelevant("alt.bad.group")

    monkeypatch.setattr(
        config, "NNTP_GROUPS", ["alt.good.group", "alt.bad.group"], raising=False
    )

    processed: list[str] = []

    class DummyClient:
        def connect(self) -> None:  # pragma: no cover - trivial
            pass

        def high_water_mark(self, group: str) -> int:  # pragma: no cover - simple
            return 1

        def xover(self, group: str, start: int, end: int):  # pragma: no cover - simple
            processed.append(group)
            return [{"subject": "Example"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: None)
    monkeypatch.setattr(
        loop,
        "insert_release",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(loop, "bulk_index_releases", lambda *_args, **_kwargs: None)

    with caplog.at_level(logging.INFO):
        loop.run_once()

    assert processed == ["alt.good.group"]
    assert any(
        r.name == "nzbidx_ingest.ingest_loop"
        and r.levelno == logging.INFO
        and getattr(r, "event", "") == "ingest_summary"
        for r in caplog.records
    )


def test_network_failure_does_not_mark_irrelevant(tmp_path, monkeypatch) -> None:
    """Groups remain eligible when the NNTP server is unreachable."""
    import nzbidx_ingest.ingest_loop as loop
    from nzbidx_ingest import config, cursors

    db_path = tmp_path / "cursors.sqlite"
    monkeypatch.setattr(config, "CURSOR_DB", str(db_path))
    monkeypatch.setattr(cursors, "CURSOR_DB", str(db_path))

    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.offline"], raising=False)

    class DummyClient:
        def connect(self) -> None:  # pragma: no cover - trivial
            pass

        def high_water_mark(self, group: str) -> int:  # pragma: no cover - simple
            return 0

        def xover(self, group: str, start: int, end: int):  # pragma: no cover - simple
            return []

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: None)

    loop.run_once()

    assert cursors.get_irrelevant_groups() == []


def test_batch_throttle_on_latency(monkeypatch) -> None:
    """run_once should backoff when avg DB latency exceeds threshold."""
    import nzbidx_ingest.ingest_loop as loop
    from nzbidx_ingest import config, cursors
    import time as _time

    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())
    monkeypatch.setattr(loop, "INGEST_DB_LATENCY_MS", 0, raising=False)
    monkeypatch.setattr(loop, "INGEST_SLEEP_MS", 10, raising=False)

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{"subject": "Example", ":bytes": "123"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: None)

    real_sleep = _time.sleep
    sleeps: list[float] = []
    monkeypatch.setattr(loop.time, "sleep", lambda s: sleeps.append(s))

    def fake_insert(*_args, **_kwargs):
        real_sleep(0.001)
        return True

    monkeypatch.setattr(loop, "insert_release", fake_insert)
    monkeypatch.setattr(loop, "bulk_index_releases", lambda *_a, **_k: None)

    loop.run_once()

    assert sleeps and sleeps[0] == 0.01
