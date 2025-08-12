"""Consolidated tests for core nzbidx functionality."""

from __future__ import annotations

import importlib
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

# ruff: noqa: E402 - path manipulation before imports

# Ensure local packages are importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))
sys.path.append(str(REPO_ROOT / "services" / "ingest" / "src"))

from nzbidx_api import nzb_builder, newznab, search as search_mod  # type: ignore
import nzbidx_ingest.main as main  # type: ignore
from nzbidx_ingest.main import CATEGORY_MAP, _infer_category, connect_db  # type: ignore


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
        nzb_builder, "nntplib", SimpleNamespace(NNTP=DummyNNTP, NNTP_SSL=DummyNNTP)
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
        nzb_builder, "nntplib", SimpleNamespace(NNTP=AutoNNTP, NNTP_SSL=AutoNNTP)
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "msg1@example.com" in xml
    assert "msg2@example.com" in xml


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

    class DummyCtx:
        def __enter__(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
            return None

    class DummyEngine:
        def begin(self) -> DummyCtx:  # pragma: no cover - trivial
            return DummyCtx()

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

    class DummyCtx:
        def __enter__(self) -> DummyConn:  # pragma: no cover - trivial
            return DummyConn()

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
            return None

    class DummyEngine:
        def begin(self) -> DummyCtx:  # pragma: no cover - trivial
            return DummyCtx()

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
