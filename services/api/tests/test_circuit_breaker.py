from __future__ import annotations

import time
from pathlib import Path
import sys

from starlette.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
import nzbidx_api.main as main  # noqa: E402
from nzbidx_api import search, newznab, search_cache  # noqa: E402
from nzbidx_api import middleware_circuit as mc  # noqa: E402
import importlib


def _client(monkeypatch) -> TestClient:
    monkeypatch.delenv("API_KEYS", raising=False)
    importlib.reload(main)
    return TestClient(main.app)


def _patch_breaker(monkeypatch, name: str) -> mc.CircuitBreaker[object]:
    breaker = mc.CircuitBreaker(max_failures=1, reset_seconds=0.1)
    monkeypatch.setattr(mc, name, breaker)
    # Update any modules that imported the breaker directly
    monkeypatch.setattr(search, name, breaker, raising=False)
    monkeypatch.setattr(newznab, name, breaker, raising=False)
    monkeypatch.setattr(search_cache, name, breaker, raising=False)
    return breaker


def test_opensearch_breaker(monkeypatch, capsys) -> None:
    client = _client(monkeypatch)
    breaker = _patch_breaker(monkeypatch, "os_breaker")

    class FailingClient:
        def search(self, *args, **kwargs):
            raise RuntimeError("boom")

    monkeypatch.setattr(main, "opensearch", FailingClient())

    resp1 = client.get("/api?t=search&q=test")
    assert resp1.status_code == 200
    assert b"<item>" not in resp1.content
    capsys.readouterr()
    resp2 = client.get("/api?t=search&q=test")
    captured = capsys.readouterr().err
    assert "breaker_open" in captured
    assert resp2.status_code == 200
    assert breaker.is_open()

    time.sleep(0.2)

    class OkClient:
        def search(self, *args, **kwargs):
            return {"hits": {"hits": []}}

    main.opensearch = OkClient()
    resp3 = client.get("/api?t=search&q=test")
    assert resp3.status_code == 200
    assert not breaker.is_open()


def test_redis_breaker(monkeypatch) -> None:
    client = _client(monkeypatch)
    breaker = _patch_breaker(monkeypatch, "redis_breaker")

    class BadCache:
        def get(self, key):
            raise RuntimeError("boom")

        def setex(self, key, ttl, val):
            raise RuntimeError("boom")

    monkeypatch.setattr(main, "cache", BadCache())

    resp1 = client.get("/api?t=getnzb&id=1")
    assert resp1.status_code == 200
    assert breaker.is_open()

    resp2 = client.get("/api?t=getnzb&id=1")
    assert resp2.status_code == 503

    time.sleep(0.2)

    class GoodCache:
        def get(self, key):
            return b"nzb"

        def setex(self, key, ttl, val):
            return None

    main.cache = GoodCache()
    resp3 = client.get("/api?t=getnzb&id=1")
    assert resp3.status_code == 200
    assert not breaker.is_open()
