from __future__ import annotations

import pytest

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_limit_too_high(monkeypatch) -> None:
    monkeypatch.setattr(search_mod, "engine", None)
    with pytest.raises(ValueError):
        search_mod.search_releases(None, limit=search_mod.MAX_LIMIT + 1)
