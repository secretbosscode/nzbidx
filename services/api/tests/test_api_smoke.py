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


def test_health_and_api_endpoints(monkeypatch):
    """Basic checks for ``/health`` and ``/api`` endpoints."""
    monkeypatch.setattr("nzbidx_api.main._os_search", _fake_search)
    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        payload = res.json()
        for key in ("status", "db", "os", "redis"):
            assert key in payload

        res = client.get("/api", params={"t": "caps"})
        assert res.status_code == 200
        assert "<caps>" in res.text

        res = client.get("/api", params={"t": "search", "q": "test"})
        assert res.status_code == 200
        assert "<rss" in res.text


def test_api_key_protection(monkeypatch):
    monkeypatch.setenv("API_KEYS", "devkey")
    import importlib, nzbidx_api.main as main

    secured_app = importlib.reload(main).app

    with TestClient(secured_app) as client:
        res = client.get("/api", params={"t": "caps"})
        assert res.status_code == 401
        res = client.get(
            "/api", params={"t": "caps"}, headers={"X-Api-Key": "devkey"}
        )
        assert res.status_code == 200
