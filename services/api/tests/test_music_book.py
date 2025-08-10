"""Tests for music and book search endpoints."""

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


def test_music_rss() -> None:
    """``/api?t=music`` should return an RSS feed."""
    request = DummyRequest(b"t=music&q=test")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert "<rss" in resp.body.decode()


def test_book_rss() -> None:
    """``/api?t=book`` should return an RSS feed."""
    request = DummyRequest(b"t=book&q=test")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert "<rss" in resp.body.decode()
