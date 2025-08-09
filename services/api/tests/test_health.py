"""Tests for the API health endpoint."""

import json
from pathlib import Path
import sys
from urllib.parse import parse_qs
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import health  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes = b""):
        self.query_params = {k: v[0] for k, v in parse_qs(query_string.decode()).items()}


def test_health() -> None:
    """``/health`` should return a simple status payload."""
    request = DummyRequest()
    response = asyncio.run(health(request))
    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["status"] == "ok"
    assert "db" in payload
