from __future__ import annotations

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_invalid_sort(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_text(query: str) -> str:
        captured["query"] = query
        return query

    class _FakeResult:
        def fetchall(self) -> list[object]:
            return []

    class _FakeConn:
        async def execute(self, sql: str, params: dict[str, object]) -> _FakeResult:
            return _FakeResult()

        async def __aenter__(self) -> "_FakeConn":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class _FakeEngine:
        def connect(self) -> _FakeConn:
            return _FakeConn()

    monkeypatch.setattr(search_mod, "get_engine", lambda: _FakeEngine())
    monkeypatch.setattr(search_mod, "text", fake_text)
    monkeypatch.setattr(search_mod, "ORDER_MAP", {"date": "posted_at"})

    search_mod.search_releases(None, limit=1, sort="invalid")

    assert f"ORDER BY {search_mod.ORDER_MAP['date']} DESC" in captured["query"]
