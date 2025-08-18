from __future__ import annotations

import importlib


def test_ingest_config_defaults(monkeypatch) -> None:
    """Ensure defaults reflect benchmark-tuned values."""
    monkeypatch.delenv("INGEST_BATCH", raising=False)
    monkeypatch.delenv("INGEST_BATCH_MIN", raising=False)
    monkeypatch.delenv("INGEST_BATCH_MAX", raising=False)
    monkeypatch.delenv("INGEST_POLL_MIN_SECONDS", raising=False)
    monkeypatch.delenv("INGEST_POLL_MAX_SECONDS", raising=False)
    import nzbidx_ingest.config as config

    importlib.reload(config)
    assert config.INGEST_BATCH == 1000
    assert config.INGEST_BATCH_MIN == 100
    assert config.INGEST_BATCH_MAX == 1000
    assert config.INGEST_POLL_MIN_SECONDS == 5
    assert config.INGEST_POLL_MAX_SECONDS == 60
