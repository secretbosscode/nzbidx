import logging
from contextlib import nullcontext
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import search as search_mod


class DummyClient:
    def search(self, **kwargs):  # pragma: no cover - not executed
        raise AssertionError("client.search should not be called")


def _patch_common(monkeypatch):
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())


def test_network_error(monkeypatch, caplog):
    _patch_common(monkeypatch)

    def dummy_call_with_retry(*args, **kwargs):
        raise search_mod.OSConnectionError("boom")

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ConnectionError):
            search_mod.search_releases(DummyClient(), {"must": []}, limit=1)
    assert "opensearch_network_error" in caplog.text


def test_timeout_error(monkeypatch, caplog):
    _patch_common(monkeypatch)

    def dummy_call_with_retry(*args, **kwargs):
        raise search_mod.ConnectionTimeout("boom")

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(TimeoutError):
            search_mod.search_releases(DummyClient(), {"must": []}, limit=1)
    assert "opensearch_timeout" in caplog.text


def test_query_error(monkeypatch, caplog):
    _patch_common(monkeypatch)

    def dummy_call_with_retry(*args, **kwargs):
        raise search_mod.TransportError("boom")

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            search_mod.search_releases(DummyClient(), {"must": []}, limit=1)
    assert "opensearch_query_error" in caplog.text
