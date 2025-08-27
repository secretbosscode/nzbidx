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


def test_groups_loaded_lazily(monkeypatch) -> None:
    """NNTP groups should not be loaded during module import."""

    import nzbidx_ingest.nntp_client as client_mod

    called_client = {"count": 0}

    class DummyClient:
        def __init__(self, *_a, **_kw) -> None:  # pragma: no cover - simple
            called_client["count"] += 1

        def list_groups(self, _pattern):  # pragma: no cover - simple
            return []

    monkeypatch.setattr(client_mod, "NNTPClient", DummyClient)
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    import nzbidx_ingest.config as config

    importlib.reload(config)

    assert called_client["count"] == 0

    load_called = {"count": 0}

    def fake_load() -> list[str]:
        load_called["count"] += 1
        return ["alt.test"]

    monkeypatch.setattr(config, "_load_groups", fake_load)
    assert config.get_nntp_groups() == ["alt.test"]
    assert load_called["count"] == 1


def test_load_groups_uses_wildcard(monkeypatch) -> None:
    import nzbidx_ingest.config as config

    called: dict[str, object] = {}

    class DummyClient:
        def __init__(self, _settings=None) -> None:  # pragma: no cover - trivial
            pass

        def list_groups(self, pattern):  # pragma: no cover - simple
            called["pattern"] = pattern
            return []

    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setattr(config, "NNTP_GROUP_WILDCARD", "alt.custom.*", raising=False)
    monkeypatch.setattr(config, "NNTPClient", DummyClient)

    config._load_groups()

    assert called["pattern"] == "alt.custom.*"
