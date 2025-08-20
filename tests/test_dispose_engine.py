from __future__ import annotations

import asyncio

from nzbidx_api import db


def test_dispose_engine_handles_closed_loop(monkeypatch):
    disposed = False

    class DummyEngine:
        async def dispose(self):
            nonlocal disposed
            disposed = True

    async def get_loop() -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    loop = asyncio.run(get_loop())
    assert loop.is_closed()

    monkeypatch.setattr(db, "_engine", DummyEngine())
    monkeypatch.setattr(db, "_engine_loop", loop)

    asyncio.run(db.dispose_engine())

    assert disposed
    assert db._engine is None
    assert db._engine_loop is None
