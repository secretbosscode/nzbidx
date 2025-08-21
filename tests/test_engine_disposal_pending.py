from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from nzbidx_api import db


async def _run_pending_disposal_test(monkeypatch):
    monkeypatch.setattr(db, "DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    await db.init_engine()
    engine = db.get_engine()
    assert engine is not None

    conn = await engine.connect()

    async def use_conn() -> None:
        await conn.execute(text("SELECT 1"))
        await conn.commit()
        await asyncio.sleep(0.2)

    task = asyncio.create_task(use_conn())
    await asyncio.sleep(0.1)
    assert not conn.closed

    try:
        await db.dispose_engine()
    except (
        RuntimeError,
        db.InternalClientError,
    ) as exc:  # pragma: no cover - failure path
        pytest.fail(f"dispose_engine raised {exc!r}")

    await task
    try:
        await conn.close()
    except Exception:  # pragma: no cover - connection may already be closed
        pass

    assert conn.closed
    assert db.get_engine() is None


def test_dispose_engine_with_pending_tasks(monkeypatch):
    asyncio.run(_run_pending_disposal_test(monkeypatch))
