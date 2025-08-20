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


def test_group_file_ignored(monkeypatch, tmp_path) -> None:
    groups = tmp_path / "groups.txt"
    groups.write_text("alt.good\nalt.bad\nalt.other\n", encoding="utf-8")
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setenv("NNTP_GROUP_FILE", str(groups))
    monkeypatch.setenv("NNTP_IGNORE_GROUPS", "alt.bad")
    import nzbidx_ingest.config as config
    importlib.reload(config)
    result = [g for g in config.NNTP_GROUPS if g not in config.IGNORE_GROUPS]
    assert result == ["alt.good", "alt.other"]
