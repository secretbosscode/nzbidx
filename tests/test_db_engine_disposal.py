from __future__ import annotations

import asyncio

import asyncpg

from nzbidx_api import db


def test_dispose_engine_after_loop_closed(monkeypatch):
    terminated = False

    class DummyConn:
        def terminate(self) -> None:
            nonlocal terminated
            terminated = True

        async def close(self):  # pragma: no cover - should not run
            raise asyncpg.InternalClientError("close called")

    class DummyRec:
        def __init__(self) -> None:
            self.dbapi_connection = DummyConn()

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

    def fake_create_async_engine(*args, **kwargs):
        return DummyEngine()

    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(db.init_engine())
    loop.close()

    asyncio.run(db.dispose_engine())

    assert terminated
    assert db._engine is None
    assert db._engine_loop is None


def test_dispose_engine_after_loop_closed_with_proxy(monkeypatch):
    terminated = False

    class DummyRawConn:
        def terminate(self) -> None:
            nonlocal terminated
            terminated = True

    class DummyConnProxy:
        def __init__(self) -> None:
            self._connection = DummyRawConn()

        def terminate(self) -> None:  # pragma: no cover - should not run
            raise asyncpg.InternalClientError("proxy terminate called")

        async def close(self):  # pragma: no cover - should not run
            raise asyncpg.InternalClientError("close called")

    class DummyRec:
        def __init__(self) -> None:
            self.dbapi_connection = DummyConnProxy()

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

    def fake_create_async_engine(*args, **kwargs):
        return DummyEngine()

    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(db.init_engine())
    loop.close()

    asyncio.run(db.dispose_engine())

    assert terminated
    assert db._engine is None
    assert db._engine_loop is None
