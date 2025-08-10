from pathlib import Path
import sys

from starlette.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import app  # noqa: E402


def test_smoke_local():
    client = TestClient(app)
    rid = "test-123"
    res = client.get("/health", headers={"X-Request-ID": rid})
    assert res.headers["X-Request-ID"] == rid
    assert res.json()["request_id"] == rid
    assert client.get("/api", params={"t": "caps"}).status_code == 200
