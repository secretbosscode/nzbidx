"""Tests for the caps endpoint."""

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


def test_caps() -> None:
    """``/api?t=caps`` should return caps XML."""
    request = DummyRequest(b"t=caps")
    response = asyncio.run(api(request))
    assert response.status_code == 200
    assert "<caps>" in response.body.decode()
