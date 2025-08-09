"""Tests for TV and movie search endpoints."""

from starlette.testclient import TestClient
from pathlib import Path
import sys

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


client = TestClient(app)


def test_tvsearch_rss() -> None:
    """``/api?t=tvsearch`` should return an RSS feed."""
    resp = client.get("/api", params={"t": "tvsearch", "q": "test"})
    assert resp.status_code == 200
    assert "<rss" in resp.text


def test_movie_rss() -> None:
    """``/api?t=movie`` should return an RSS feed."""
    resp = client.get("/api", params={"t": "movie", "q": "test"})
    assert resp.status_code == 200
    assert "<rss" in resp.text
