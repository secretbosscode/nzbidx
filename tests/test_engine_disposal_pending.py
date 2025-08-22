from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from nzbidx_api import db

pytest.importorskip("aiosqlite")


async def _use_conn(conn) -> None:
    await conn.execute(text("SELECT 1"))
    await conn.commit()
    await asyncio.sleep(0.2)


def test_dispose_engine_with_pending_tasks(monkeypatch):
    monkeypatch.setattr(db, "DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(db.init_engine())
    engine = db.get_engine()
    assert engine is not None

    conn = loop.run_until_complete(engine.connect())

    task = loop.create_task(_use_conn(conn))
    loop.run_until_complete(asyncio.sleep(0.1))
    assert not conn.closed

    # Ensure any pending tasks are handled before disposing the engine
    task.cancel()
    loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
    assert task.cancelled()

    asyncio.set_event_loop(None)
    try:
        asyncio.run(db.dispose_engine())
    except (
        RuntimeError,
        db.InternalClientError,
    ) as exc:  # pragma: no cover - failure path
        pytest.fail(f"dispose_engine raised {exc!r}")

    assert db.get_engine() is None
    try:
        loop.run_until_complete(conn.close())
    except Exception:  # pragma: no cover - connection may already be closed
        pass
    assert conn.closed
    loop.close()
