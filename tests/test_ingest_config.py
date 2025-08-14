from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sys
from pathlib import Path
import importlib

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))


def test_ingest_config_defaults(monkeypatch) -> None:
    """Ensure defaults reflect benchmark-tuned values."""
    monkeypatch.delenv("INGEST_BATCH", raising=False)
    monkeypatch.delenv("INGEST_BATCH_MIN", raising=False)
    monkeypatch.delenv("INGEST_BATCH_MAX", raising=False)
    monkeypatch.delenv("INGEST_POLL_SECONDS", raising=False)
    import nzbidx_ingest.config as config
    importlib.reload(config)
    assert config.INGEST_BATCH == 1000
    assert config.INGEST_BATCH_MIN == 100
    assert config.INGEST_BATCH_MAX == 1000
    assert config.INGEST_POLL_SECONDS == 30
