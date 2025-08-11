from pathlib import Path
import sys

from starlette.testclient import TestClient

# Ensure local packages are importable
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api.main import app  # type: ignore  # noqa: E402


def test_health_endpoint() -> None:
    """Basic smoke test for CI to ensure app responds."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200


def test_api_health_endpoint() -> None:
    """Ensure namespaced health endpoint responds."""
    with TestClient(app) as client:
        response = client.get("/api/health")
        assert response.status_code == 200


def test_status_endpoint() -> None:
    """Ensure status endpoint responds."""
    with TestClient(app) as client:
        response = client.get("/api/status")
        assert response.status_code == 200
