from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path

import pytest

# ruff: noqa: E402 - path manipulation before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_limit_too_high(monkeypatch) -> None:
    class DummyClient:
        def search(self, **kwargs):  # pragma: no cover - should not be called
            raise AssertionError("search should not be called")

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    with pytest.raises(ValueError):
        search_mod.search_releases(client, {"must": []}, limit=search_mod.MAX_LIMIT + 1)


def test_search_releases_offset_clamped(monkeypatch) -> None:
    class DummyClient:
        def __init__(self) -> None:
            self.from_value: int | None = None

        def search(self, **kwargs):
            self.from_value = kwargs["body"]["from"]
            return {"hits": {"hits": []}}

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    search_mod.search_releases(
        client,
        {"must": []},
        limit=1,
        offset=search_mod.MAX_OFFSET + 1,
    )
    assert client.from_value == search_mod.MAX_OFFSET
