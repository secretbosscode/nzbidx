"""Tests for the caps endpoint."""

from fastapi.testclient import TestClient
from pathlib import Path
import sys

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


client = TestClient(app)


def test_caps() -> None:
    """``/api?t=caps`` should return caps XML."""
    response = client.get("/api", params={"t": "caps"})
    assert response.status_code == 200
    assert "<caps>" in response.text
