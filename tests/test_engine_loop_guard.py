from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from nzbidx_api import db

pytest.importorskip("aiosqlite")


async def _attempt_conn():
    engine = db.get_engine()
    assert engine is not None
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


def test_engine_loop_guard(monkeypatch):
    monkeypatch.setattr(db, "DATABASE_URL", "sqlite+aiosqlite:///:memory:")

    loop1 = asyncio.new_event_loop()
    asyncio.set_event_loop(loop1)
    loop1.run_until_complete(db.init_engine())
    loop1.close()

    loop2 = asyncio.new_event_loop()
    asyncio.set_event_loop(loop2)
    with pytest.raises(RuntimeError):
        loop2.run_until_complete(_attempt_conn())

    loop2.run_until_complete(db.init_engine())
    loop2.run_until_complete(db.dispose_engine())
    asyncio.set_event_loop(None)
    loop2.close()
