"""Tests for TV and movie search endpoints."""

from pathlib import Path
import sys
from urllib.parse import parse_qs
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import api  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes):
        self.query_params = {
            k: v[0] for k, v in parse_qs(query_string.decode()).items()
        }
        self.headers = {}


def test_tvsearch_rss() -> None:
    """``/api?t=tvsearch`` should return an RSS feed."""
    request = DummyRequest(b"t=tvsearch&q=test")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert "<rss" in resp.body.decode()


def test_movie_rss() -> None:
    """``/api?t=movie`` should return an RSS feed."""
    request = DummyRequest(b"t=movie&q=test")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert "<rss" in resp.body.decode()
