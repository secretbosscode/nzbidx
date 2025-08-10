"""Tests for presence of security headers."""

from pathlib import Path
import sys

from starlette.testclient import TestClient

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


def test_security_headers() -> None:
    client = TestClient(main.app)
    resp = client.get("/health")
    headers = {k.lower(): v for k, v in resp.headers.items()}
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["referrer-policy"] == "no-referrer"
    assert headers["x-frame-options"] == "DENY"  # uppercase check
    assert headers["x-download-options"] == "noopen"
    assert headers["permissions-policy"] == "interest-cohort=()"
