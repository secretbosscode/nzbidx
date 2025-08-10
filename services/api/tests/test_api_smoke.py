"""End-to-end smoke tests for the API service."""

from pathlib import Path
import sys

from starlette.testclient import TestClient

# Ensure importable package
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # type: ignore  # noqa: E402


def _fake_search(*args, **kwargs):
    return [
        {
            "title": "Test Item",
            "guid": "1",
            "pubDate": "Fri, 01 Jan 2021 00:00:00 GMT",
            "category": "1000",
            "link": "/nzb/1",
        }
    ]


def test_health_and_caps(monkeypatch):
    """``/health`` returns expected keys and caps hides adult category."""
    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        payload = res.json()
        for key in ("status", "db", "os", "redis"):
            assert key in payload

        monkeypatch.setenv("ALLOW_XXX", "false")
        res = client.get("/api", params={"t": "caps"})
        assert res.status_code == 200
        assert '<category id="6000"' not in res.text

        res = client.get("/api", params={"t": "search", "q": "x", "cat": "6000"})
        assert "adult content disabled" in res.text


def test_api_key_protection(monkeypatch):
    monkeypatch.setenv("API_KEYS", "dev")
    import importlib
    import nzbidx_api.main as main

    secured_app = importlib.reload(main).app

    with TestClient(secured_app) as client:
        res = client.get("/api", params={"t": "caps"})
        assert res.status_code == 401
        res = client.get("/api", params={"t": "caps"}, headers={"X-Api-Key": "dev"})
        assert res.status_code == 200


def test_search_uses_cache(monkeypatch):
    store: dict[str, str] = {}

    monkeypatch.setattr("nzbidx_api.main.get_cached_rss", lambda k: store.get(k))

    def fake_cache(key: str, xml: str) -> None:
        store[key] = xml

    monkeypatch.setattr("nzbidx_api.main.cache_rss", fake_cache)

    calls = {"count": 0}

    def fake_search(*args, **kwargs):
        calls["count"] += 1
        return _fake_search()

    monkeypatch.setattr("nzbidx_api.main._os_search", fake_search)

    with TestClient(app) as client:
        res1 = client.get("/api", params={"t": "search", "q": "test"})
        assert res1.status_code == 200
        res2 = client.get("/api", params={"t": "search", "q": "test"})
        assert res2.status_code == 200
        assert calls["count"] == 1
