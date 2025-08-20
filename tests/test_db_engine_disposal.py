from __future__ import annotations

import asyncio
import logging

from nzbidx_api import db


def test_dispose_engine_after_loop_closed(monkeypatch, caplog):
    disposed = False

    class DummyEngine:
        async def dispose(self):
            nonlocal disposed
            disposed = True

    engine = DummyEngine()
    loop = asyncio.new_event_loop()
    loop.close()

    monkeypatch.setattr(db, "_engine", engine)
    monkeypatch.setattr(db, "_engine_loop", loop)

    caplog.set_level(logging.WARNING)
    asyncio.run(db.dispose_engine())

    assert disposed
    assert "engine_loop_closed" in caplog.text
    assert db._engine is None
    assert db._engine_loop is None
