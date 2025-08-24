from __future__ import annotations

from nzbidx_api import search as search_mod  # type: ignore
from types import SimpleNamespace
from datetime import datetime


class _FakeResult:
    def __init__(self, rows: list[object] | None = None, *, scalar: bool | None = None):
        self._rows = rows or []
        self._scalar = scalar

    def fetchall(self) -> list[object]:
        return self._rows

    def scalar(self) -> bool | None:
        return self._scalar


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    async def execute(
        self, sql: str, params: dict[str, object] | None = None
    ) -> _FakeResult:
        if "pg_attribute" in sql:
            return _FakeResult(scalar=True)
        self._engine.sql = sql
        self._engine.params = params or {}
        return _FakeResult(self._engine.rows)

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def __init__(self, rows: list[object] | None = None) -> None:
        self.rows = rows or []

    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def _setup_engine(monkeypatch, rows: list[object] | None = None) -> _FakeEngine:
    engine = _FakeEngine(rows)
    monkeypatch.setattr(search_mod, "get_engine", lambda: engine)
    monkeypatch.setattr(search_mod, "text", lambda s: s)
    return engine


def test_query_with_punctuation(monkeypatch) -> None:
    engine = _setup_engine(monkeypatch)
    q = "c++ movies"
    assert search_mod.search_releases(q, limit=1) == []
    assert "plainto_tsquery('simple', :tsquery)" in engine.sql
    assert engine.params["tsquery"] == q


def test_query_with_reserved_operator(monkeypatch) -> None:
    engine = _setup_engine(monkeypatch)
    q = "foo | bar"
    search_mod.search_releases(q, limit=1)
    assert "plainto_tsquery('simple', :tsquery)" in engine.sql
    assert engine.params["tsquery"] == q


def test_zero_size_release_not_returned(monkeypatch) -> None:
    row = SimpleNamespace(
        id=1,
        norm_title="zero",
        category="cat",
        size_bytes=0,
        posted_at=datetime.now(),
    )
    engine = _setup_engine(monkeypatch, rows=[row])
    assert search_mod.search_releases(None, limit=1) == []
    assert "size_bytes > 0" in engine.sql
