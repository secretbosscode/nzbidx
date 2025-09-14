import asyncio
import logging
from types import SimpleNamespace

from cachetools import TTLCache

from nzbidx_api import main as api_main, search as search_mod, search_cache


class _FailConn:
    async def __aenter__(self):
        raise OSError("db down")

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FailEngine:
    def connect(self):
        return _FailConn()


def test_persistent_db_failure_returns_503_and_logs(monkeypatch, caplog):
    search_cache._CACHE = TTLCache(
        maxsize=search_cache.settings.search_cache_max_entries,
        ttl=search_cache.settings.search_ttl_seconds,
    )
    engine = _FailEngine()
    monkeypatch.setattr(api_main, "get_engine", lambda: engine)
    monkeypatch.setattr(search_mod, "get_engine", lambda: engine)
    monkeypatch.setattr(search_mod, "text", lambda s: s)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(search_mod.asyncio, "sleep", no_sleep)

    req = SimpleNamespace(
        query_params={"t": "search"}, headers={"Cache-Control": "no-cache"}
    )
    with caplog.at_level(logging.WARNING, logger="nzbidx_api.search"):
        resp = asyncio.run(api_main.api(req))
    assert resp.status_code == 503
    assert any(rec.message == "search_retry" for rec in caplog.records)
