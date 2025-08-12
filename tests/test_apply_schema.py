from __future__ import annotations

import sys
from pathlib import Path

import asyncio

# ruff: noqa: E402 - path manipulation before imports
# Ensure the API package is importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import db


def test_apply_schema_creates_database(monkeypatch):
    executed: list[str] = []
    admin_urls: list[tuple[str, str | None]] = []
    admin_exec: list[str] = []

    class DummyConn:
        def __init__(self, engine):
            self.engine = engine

        async def __aenter__(self):
            self.engine.calls += 1
            if self.engine.calls == 1:
                raise Exception("database does not exist")
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            return None

        async def rollback(self):
            return None

    class DummyEngine:
        def __init__(self):
            self.calls = 0

        def connect(self):
            return DummyConn(self)

    class AdminConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            admin_exec.append(stmt)

    class AdminEngine:
        def connect(self):
            return AdminConn()

        async def dispose(self):
            return None

    def fake_create_async_engine(url, echo=False, isolation_level=None):
        admin_urls.append((url, isolation_level))
        return AdminEngine()

    monkeypatch.setattr(db, "engine", DummyEngine())
    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "DATABASE_URL", "postgresql+asyncpg://u@h/db")

    asyncio.run(db.apply_schema())

    assert admin_urls == [("postgresql+asyncpg://u@h/postgres", "AUTOCOMMIT")]
    assert any(stmt.startswith("CREATE DATABASE") for stmt in admin_exec)
    assert executed  # schema statements executed after creation
