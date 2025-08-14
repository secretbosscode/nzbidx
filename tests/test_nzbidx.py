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


class DummyNNTP:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):  # pragma: no cover - trivial
        return self

    def __exit__(self, _exc_type, _exc, _tb):  # pragma: no cover - trivial
        pass

    def group(self, group):
        # resp, count, first, last, name
        return ("", 2, "1", "2", group)

    def xover(self, start, end):
        return (
            "",
            [
                {
                    "subject": 'MyRelease "testfile.bin" (1/2)',
                    "message-id": "msg1@example.com",
                },
                {
                    "subject": 'MyRelease "testfile.bin" (2/2)',
                    "message-id": "msg2@example.com",
                },
            ],
        )

    def body(self, message_id, decode=False):  # pragma: no cover - simple
        if message_id == "msg1@example.com":
            lines = [b"a" * 123]
        else:
            lines = [b"b" * 456]
        return "", 0, message_id, lines


class AutoNNTP(DummyNNTP):
    def list(self):
        return "", [("alt.binaries.example", "0", "0", "0")]


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
    assert json.loads(lines[2]) == {
        "index": {"_index": OS_RELEASES_ALIAS, "_id": "id2"}
    }
    assert json.loads(lines[3])["norm_title"] == "two"
    assert captured["refresh"] is False


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
    """Searches without parameters should return no results."""
    called = False

    def dummy_search(*args, **kwargs):  # pragma: no cover - should not be called
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(api_main, "search_releases", dummy_search)
    monkeypatch.setattr(api_main, "opensearch", object())
    assert api_main._os_search(None, category="2040") == []
    assert called is False


def test_os_search_ignores_whitespace_query(monkeypatch) -> None:
    """Whitespace-only queries should be treated as missing."""
    called = False

    def dummy_search(*args, **kwargs):  # pragma: no cover - should not be called
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(api_main, "search_releases", dummy_search)
    monkeypatch.setattr(api_main, "opensearch", object())
    assert api_main._os_search("   ", category="2040") == []
    assert called is False


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


def test_failed_fetch_cached(monkeypatch) -> None:
    cache = DummyCache()
    calls: list[str] = []

    def boom(release_id: str) -> str:
        calls.append(release_id)
        raise RuntimeError("boom")

    monkeypatch.setattr(newznab.nzb_builder, "build_nzb_for_release", boom)

    key = "nzb:123"
    # first call populates failure sentinel
    try:
        newznab.get_nzb("123", cache)
    except newznab.NzbFetchError:
        pass
    assert cache.store[key] == newznab.FAIL_SENTINEL
    assert calls == ["123"]

    calls.clear()
    # second call should hit cache and not invoke builder
    try:
        newznab.get_nzb("123", cache)
    except newznab.NzbFetchError:
        pass
    assert calls == []


def test_builds_nzb(monkeypatch) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")
    monkeypatch.setattr(
        nzb_builder,
        "nntplib",
        SimpleNamespace(NNTP=DummyNNTP, NNTP_SSL=DummyNNTP, NNTP_SSL_PORT=563),
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "msg1@example.com" in xml
    assert "msg2@example.com" in xml
    assert '<segment bytes="123" number="1">msg1@example.com</segment>' in xml
    assert '<segment bytes="456" number="2">msg2@example.com</segment>' in xml


def test_builds_nzb_auto_groups(monkeypatch) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setattr(
        nzb_builder,
        "nntplib",
        SimpleNamespace(NNTP=AutoNNTP, NNTP_SSL=AutoNNTP, NNTP_SSL_PORT=563),
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "msg1@example.com" in xml
    assert "msg2@example.com" in xml


def test_builds_nzb_auto_ssl(monkeypatch) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_PORT", "563")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")
    monkeypatch.delenv("NNTP_SSL", raising=False)

    used: dict[str, str] = {}

    class Plain(DummyNNTP):
        def __init__(self, *_args, **_kwargs):
            used["cls"] = "plain"
            super().__init__(*_args, **_kwargs)

    class Secure(DummyNNTP):
        def __init__(self, *_args, **_kwargs):
            used["cls"] = "ssl"
            super().__init__(*_args, **_kwargs)

    monkeypatch.setattr(
        nzb_builder,
        "nntplib",
        SimpleNamespace(NNTP=Plain, NNTP_SSL=Secure, NNTP_SSL_PORT=563),
    )
    nzb_builder.build_nzb_for_release("MyRelease")
    assert used["cls"] == "ssl"


def test_build_nzb_logs_exception(monkeypatch, caplog) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")

    class FailNNTP(DummyNNTP):
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    monkeypatch.setattr(
        nzb_builder,
        "nntplib",
        SimpleNamespace(NNTP=FailNNTP, NNTP_SSL=FailNNTP, NNTP_SSL_PORT=563),
    )
    with caplog.at_level(logging.ERROR):
        xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "nzb build failed for MyRelease" in caplog.text
    assert "<nzb" in xml


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


def test_connect_db_falls_back_to_sqlite(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")

    def fake_create_engine(url: str, echo: bool = False, future: bool = True):
        raise ModuleNotFoundError("No module named 'psycopg'")

    monkeypatch.setattr(main, "create_engine", fake_create_engine)
    monkeypatch.setattr(main, "text", lambda s: s)

    conn = connect_db()
    # The fallback connection should be a functioning SQLite database.
    conn.execute("SELECT 1")
    assert conn.__class__.__module__.startswith("sqlite3")


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
    assert (
        "nzbidx_ingest.ingest_loop",
        logging.INFO,
        "ingest_summary",
    ) in caplog.record_tuples


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
            return [{"subject": "Example"}]

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
