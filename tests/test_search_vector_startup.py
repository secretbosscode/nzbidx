from __future__ import annotations

import logging
import asyncio
import pytest

from nzbidx_api import main as main_mod  # type: ignore


class DummyConn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def scalar(self, *_args, **_kwargs):
        return None


class DummyEngine:
    def connect(self):
        return DummyConn()


def test_startup_fails_when_search_vector_missing(monkeypatch, caplog):
    assert main_mod.ensure_search_vector in main_mod.app.on_startup
    dummy_engine = DummyEngine()
    monkeypatch.setattr(main_mod, "get_engine", lambda: dummy_engine)
    with caplog.at_level(logging.ERROR):
        with pytest.raises(
            RuntimeError,
            match=r"search_vector column missing; apply schema\.sql to initialize the database",
        ):
            asyncio.run(main_mod.ensure_search_vector())
    assert (
        "search_vector column missing; apply schema.sql to initialize the database"
        in caplog.text
    )
