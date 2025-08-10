"""Ensure the OpenAPI route exposes basic schema."""

from pathlib import Path
import sys

from starlette.testclient import TestClient

# Ensure importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


def test_openapi_route():
    client = TestClient(app)
    res = client.get("/openapi.json")
    assert res.status_code == 200
    data = res.json()
    assert "/api" in data.get("paths", {})
    params = data["paths"]["/api"]["get"]["parameters"]
    names = {p["name"] for p in params}
    assert "X-Request-ID" in names
    assert "X-Api-Key" in names
