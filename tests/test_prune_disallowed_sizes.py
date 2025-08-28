from __future__ import annotations

import importlib
import logging
import sys


def test_warns_when_no_size_thresholds(monkeypatch, caplog) -> None:
    monkeypatch.delenv("MIN_RELEASE_BYTES", raising=False)
    monkeypatch.delenv("MAX_RELEASE_BYTES", raising=False)
    sys.modules.pop("scripts.prune_disallowed_sizes", None)
    mod = importlib.import_module("scripts.prune_disallowed_sizes")
    with caplog.at_level(logging.WARNING):
        assert mod.prune_sizes() == 0
    assert any(
        "built-in defaults" in record.message and "0 rows considered" in record.message
        for record in caplog.records
    )
