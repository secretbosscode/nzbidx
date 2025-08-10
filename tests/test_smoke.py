from pathlib import Path
import sys

from starlette.testclient import TestClient

# Ensure local packages are importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.main import app  # type: ignore  # noqa: E402


def test_health_endpoint() -> None:
    """Basic smoke test for CI to ensure app responds."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
