from pathlib import Path
import sys

from starlette.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
import nzbidx_api.main as main  # noqa: E402


def test_request_id_echo() -> None:
    client = TestClient(main.app)
    resp = client.get("/health", headers={"X-Request-ID": "abc"})
    assert resp.headers["X-Request-ID"] == "abc"


def test_request_id_generated() -> None:
    client = TestClient(main.app)
    resp = client.get("/health")
    assert resp.headers.get("X-Request-ID")
