from __future__ import annotations

from nzbidx_api import search as search_mod  # type: ignore


class _FakeResult:
    def fetchall(self) -> list[object]:
        return []


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    async def execute(self, sql: str, params: dict[str, object]) -> _FakeResult:
        self._engine.sql = sql
        self._engine.params = params
        return _FakeResult()

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def _setup_engine(monkeypatch) -> _FakeEngine:
    engine = _FakeEngine()
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
