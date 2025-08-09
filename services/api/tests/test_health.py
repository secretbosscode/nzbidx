"""Tests for the API health endpoint."""

from fastapi.testclient import TestClient
from pathlib import Path
import sys

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


client = TestClient(app)


def test_health() -> None:
    """``/health`` should return a simple status payload."""
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "db" in payload
