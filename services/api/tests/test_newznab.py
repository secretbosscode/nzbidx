"""Tests for Newznab search and getnzb endpoints."""

from starlette.testclient import TestClient
from pathlib import Path
import sys

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


client = TestClient(app)


def test_search_rss() -> None:
    """``/api?t=search`` should return an RSS feed."""
    resp = client.get("/api", params={"t": "search", "q": "test"})
    assert resp.status_code == 200
    assert "<rss" in resp.text


def test_getnzb() -> None:
    """``/api?t=getnzb`` should return an NZB stub."""
    resp = client.get("/api", params={"t": "getnzb", "id": "1"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-nzb")
    assert "<nzb" in resp.text
