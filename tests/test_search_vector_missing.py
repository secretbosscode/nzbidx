from __future__ import annotations

import pytest
from sqlalchemy.exc import ProgrammingError

from nzbidx_api import main as main_mod  # type: ignore


@pytest.mark.asyncio
async def test_search_vector_missing_column(monkeypatch) -> None:
    err = ProgrammingError(
        "SELECT", {}, Exception('column "search_vector" does not exist')
    )

    async def fake_search_releases_async(*args, **kwargs):
        raise err

    monkeypatch.setattr(main_mod, "get_engine", lambda: object())
    monkeypatch.setattr(main_mod, "search_releases_async", fake_search_releases_async)

    with pytest.raises(ProgrammingError):
        await main_mod._search("test")
