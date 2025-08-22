from __future__ import annotations

import asyncio

from nzbidx_api import db


def test_dispose_engine_handles_closed_loop(monkeypatch):
    closed = False

    class DummyProtocol:
        def terminate(self) -> None:
            nonlocal closed
            closed = True

    class DummyRec:
        def __init__(self) -> None:
            self.dbapi_connection = type("Conn", (), {"_protocol": DummyProtocol()})()

    class DummyQueue:
        def __init__(self) -> None:
            self.items = [DummyRec()]

        def get_nowait(self):
            if self.items:
                return self.items.pop()
            raise Exception

    class DummyPool:
        def __init__(self) -> None:
            self._pool = DummyQueue()

    class DummyEngine:
        def __init__(self) -> None:
            self.sync_engine = type("SyncEngine", (), {"pool": DummyPool()})()

        async def dispose(self):  # pragma: no cover - should not be called
            raise RuntimeError("dispose called")

    loop = asyncio.new_event_loop()
    loop.close()

    monkeypatch.setattr(db, "_engine", DummyEngine())
    monkeypatch.setattr(db, "_engine_loop", loop)

    asyncio.run(db.dispose_engine())

    assert closed
    assert db._engine is None
    assert db._engine_loop is None


def test_dispose_engine_no_unretrieved_future_warning(monkeypatch, capsys):
    class DummyProtocol:
        def terminate(self):
            fut = asyncio.Future()
            fut.set_exception(db.InternalClientError("boom"))
            return fut

    class DummyRec:
        def __init__(self) -> None:
            self.dbapi_connection = type("Conn", (), {"_protocol": DummyProtocol()})()

    class DummyQueue:
        def __init__(self) -> None:
            self.items = [DummyRec()]

        def get_nowait(self):
            if self.items:
                return self.items.pop()
            raise Exception

    class DummyPool:
        def __init__(self) -> None:
            self._pool = DummyQueue()

    class DummyEngine:
        def __init__(self) -> None:
            self.sync_engine = type("SyncEngine", (), {"pool": DummyPool()})()

        async def dispose(self):  # pragma: no cover - should not be called
            raise RuntimeError("dispose called")

    loop = asyncio.new_event_loop()
    loop.close()

    monkeypatch.setattr(db, "_engine", DummyEngine())
    monkeypatch.setattr(db, "_engine_loop", loop)

    asyncio.run(db.dispose_engine())

    out = capsys.readouterr()
    assert "Future exception was never retrieved" not in out.err
    assert db._engine is None
    assert db._engine_loop is None
