from __future__ import annotations

import asyncio

from nzbidx_api import db


def test_dispose_engine_handles_closed_loop(monkeypatch):
    disposed = False

    class DummyEngine:
        async def dispose(self):
            nonlocal disposed
            disposed = True

    loop = asyncio.new_event_loop()
    loop.close()

    monkeypatch.setattr(db, "_engine", DummyEngine())
    monkeypatch.setattr(db, "_engine_loop", loop)

    asyncio.run(db.dispose_engine())

    assert disposed
    assert db._engine is None
    assert db._engine_loop is None
