from __future__ import annotations

from nzbidx_api import search as search_mod  # type: ignore


class _FakeResult:
    def __aiter__(self):
        async def _gen():
            return
            yield  # pragma: no cover - generator placeholder

        return _gen()


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    async def execute(self, sql: str, params: dict[str, object]) -> _FakeResult:
        self._engine.sql = sql
        return _FakeResult()

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def test_search_releases_invalid_sort(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_text(query: str) -> str:
        captured["query"] = query
        return query

    monkeypatch.setattr(search_mod, "get_engine", lambda: _FakeEngine())
    monkeypatch.setattr(search_mod, "text", fake_text)

    search_mod.search_releases(None, limit=1, sort="invalid")

    assert "ORDER BY posted_at DESC" in captured["query"]
