import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

API_SRC = ROOT / "services" / "api" / "src"
if str(API_SRC) not in sys.path:
    sys.path.insert(0, str(API_SRC))


@pytest.fixture(autouse=True)
def _nntp_env(monkeypatch) -> None:
    """Provide default NNTP configuration for tests."""

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_PORT", "119")
    monkeypatch.setenv("NNTP_USER", "user")
    monkeypatch.setenv("NNTP_PASS", "pass")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")

