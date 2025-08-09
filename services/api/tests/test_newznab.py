"""Tests for Newznab search and getnzb endpoints."""

from pathlib import Path
import sys
from urllib.parse import parse_qs
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import api  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes):
        self.query_params = {k: v[0] for k, v in parse_qs(query_string.decode()).items()}


def test_search_rss() -> None:
    """``/api?t=search`` should return an RSS feed."""
    request = DummyRequest(b"t=search&q=test")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert "<rss" in resp.body.decode()


def test_getnzb() -> None:
    """``/api?t=getnzb`` should return an NZB stub."""
    request = DummyRequest(b"t=getnzb&id=1")
    resp = asyncio.run(api(request))
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-nzb")
    assert "<nzb" in resp.body.decode()
