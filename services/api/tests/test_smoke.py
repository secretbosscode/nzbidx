import os
from pathlib import Path
import sys

from starlette.testclient import TestClient

os.environ["API_KEYS"] = "secret"

# Ensure local packages are importable
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_api.main as main  # type: ignore  # noqa: E402
from nzbidx_common.os import OS_RELEASES_ALIAS  # type: ignore  # noqa: E402

app = main.app


def test_health_endpoint() -> None:
    """Basic smoke test for CI to ensure app responds."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200


def test_takedown_deletes_release(monkeypatch) -> None:
    class DummyOS:
        def __init__(self):
            self.deleted = []

        def delete(self, *, index, id, refresh="wait_for"):
            self.deleted.append((index, id, refresh))

    dummy = DummyOS()
    monkeypatch.setattr(main, "opensearch", dummy)
    with TestClient(app) as client:
        response = client.post(
            "/api/admin/takedown",
            headers={"X-Api-Key": "secret"},
            json={"id": "abc"},
        )
        assert response.status_code == 200
        assert dummy.deleted == [(OS_RELEASES_ALIAS, "abc", "wait_for")]
