from __future__ import annotations

import logging

from nzbidx_api import search as search_mod  # type: ignore


class _FakeRow:
    def __init__(self, size_bytes: int) -> None:
        self.id = 1
        self.norm_title = "bad"
        self.category = "cat"
        self.size_bytes = size_bytes
        self.posted_at = None


class _FakeResult:
    def __init__(self, row: _FakeRow) -> None:
        self._row = row

    def fetchall(self) -> list[_FakeRow]:
        return [self._row]


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    async def execute(self, sql: str, params: dict[str, object]) -> _FakeResult:
        self._engine.sql = sql
        self._engine.params = params
        return _FakeResult(_FakeRow(0))

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def test_excludes_zero_size(monkeypatch, caplog) -> None:
    engine = _FakeEngine()
    monkeypatch.setattr(search_mod, "get_engine", lambda: engine)
    monkeypatch.setattr(search_mod, "text", lambda s: s)

    with caplog.at_level(logging.INFO):
        items = search_mod.search_releases(None, limit=1)

    assert items == []
    assert "size_bytes > 0" in engine.sql
    assert any(
        record.message == "search_invalid_size" and getattr(record, "skip_count", 0) == 1
        for record in caplog.records
    )
