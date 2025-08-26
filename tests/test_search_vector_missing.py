from __future__ import annotations

import asyncio
from types import SimpleNamespace

from cachetools import TTLCache
from nzbidx_api import main as main_mod, search as search_mod, search_cache  # type: ignore
from nzbidx_api.search import SearchVectorUnavailable  # type: ignore


class _FakeResult:
    def __init__(self, *, scalar: bool) -> None:
        self._scalar = scalar

    def scalar(self) -> bool:
        return self._scalar

    def fetchall(self):  # pragma: no cover - not used
        return []


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    async def execute(
        self, sql: str, params: dict[str, object] | None = None
    ) -> _FakeResult:
        if "pg_attribute" in sql:
            return _FakeResult(scalar=False)
        self._engine.sql = sql
        self._engine.params = params or {}
        return _FakeResult(scalar=True)

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def test_fallback_to_ilike(monkeypatch) -> None:
    engine = _FakeEngine()
    monkeypatch.setattr(search_mod, "get_engine", lambda: engine)
    monkeypatch.setattr(search_mod, "text", lambda s: s)
    search_mod.search_releases("foo", limit=1)
    assert "norm_title ILIKE" in engine.sql
    assert engine.params["title_like"] == "%foo%"


def test_api_returns_503_on_introspection_error(monkeypatch) -> None:
    search_cache._CACHE = TTLCache(
        maxsize=search_cache.settings.search_cache_max_entries,
        ttl=search_cache.settings.search_ttl_seconds,
    )
    monkeypatch.setattr(main_mod, "get_engine", lambda: object())

    async def fake_search_releases_async(*args, **kwargs):
        raise SearchVectorUnavailable("full-text search unavailable")

    monkeypatch.setattr(main_mod, "search_releases_async", fake_search_releases_async)

    req = SimpleNamespace(
        query_params={"t": "search", "q": "foo"},
        headers={"Cache-Control": "no-cache"},
    )
    resp = asyncio.run(main_mod.api(req))
    assert resp.status_code == 503
    assert b"full-text search unavailable" in resp.body
