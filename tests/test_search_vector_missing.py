from __future__ import annotations

import pytest
from sqlalchemy.exc import ProgrammingError

from nzbidx_api import main as main_mod  # type: ignore


def test_search_vector_missing_column(monkeypatch) -> None:
    err = ProgrammingError("SELECT", {}, Exception('column "search_vector" does not exist'))

    def fake_search_releases(*args, **kwargs):
        raise err

    monkeypatch.setattr(main_mod, "search_releases", fake_search_releases)

    with pytest.raises(ProgrammingError):
        main_mod._search("test")
