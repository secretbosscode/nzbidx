from __future__ import annotations

import sys
from pathlib import Path

import pytest

# ruff: noqa: E402 - path manipulation before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_limit_too_high(monkeypatch) -> None:
    monkeypatch.setattr(search_mod, "engine", None)
    with pytest.raises(ValueError):
        search_mod.search_releases(None, limit=search_mod.MAX_LIMIT + 1)

