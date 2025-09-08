from __future__ import annotations

import asyncio
from types import SimpleNamespace

from nzbidx_api import db


def test_drop_privileges_skipped_non_postgres(monkeypatch):
    executed: list[str] = []
    scalars: list[str] = []
    commits: list[str] = []

    class DummyConn:
        def __init__(self):
            self.dialect = SimpleNamespace(name="sqlite")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            commits.append("commit")

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            scalars.append(stmt)
            return True

    class DummyEngine:
        def connect(self):
            return DummyConn()

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)

    async def fake_apply_async(conn, text, statements):
        return None

    monkeypatch.setattr(db, "apply_async", fake_apply_async)
    monkeypatch.setattr(db, "load_schema_statements", lambda: [])

    asyncio.run(db.apply_schema())

    assert executed == []
    assert scalars == []
    assert commits == []


def test_drop_privileges_skipped_for_non_superuser(monkeypatch):
    executed: list[str] = []
    scalars: list[str] = []
    commits: list[str] = []

    class DummyConn:
        def __init__(self):
            self.dialect = SimpleNamespace(name="postgresql")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            commits.append("commit")

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            scalars.append(stmt)
            return False

    class DummyEngine:
        def connect(self):
            return DummyConn()

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)

    async def fake_apply_async(conn, text, statements):
        return None

    monkeypatch.setattr(db, "apply_async", fake_apply_async)
    monkeypatch.setattr(db, "load_schema_statements", lambda: [])

    asyncio.run(db.apply_schema())

    assert scalars == ["SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER"]
    assert executed == []
    assert commits == []
