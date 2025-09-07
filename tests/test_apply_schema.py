from __future__ import annotations

import asyncio

import pytest

from nzbidx_api import db
from nzbidx_ingest.db_migrations import migrate_release_adult_partitions


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

        async def scalar(self, stmt, params=None):
            return 1

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

        async def scalar(self, stmt, params=None):
            return 0  # database missing

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

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "DATABASE_URL", "postgresql+asyncpg://u@h/db")

    asyncio.run(db.apply_schema())

    assert admin_urls == [("postgresql+asyncpg://u@h/postgres", "AUTOCOMMIT")]
    assert any(stmt.startswith("CREATE DATABASE") for stmt in admin_exec)
    assert executed  # schema statements executed after creation


def test_apply_schema_retries_on_oserror(monkeypatch):
    attempts = 0

    class DummyConn:
        async def __aenter__(self):
            nonlocal attempts
            attempts += 1
            raise OSError("unreachable")

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class DummyEngine:
        def connect(self):
            return DummyConn()

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)

    asyncio.run(db.apply_schema(max_attempts=3, retry_delay=0))

    assert attempts == 3


def test_apply_schema_handles_function_with_semicolons_without_sqlparse(monkeypatch):
    executed: list[str] = []

    sql = (
        "CREATE TABLE t(a int);\n"
        "CREATE OR REPLACE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "  PERFORM 1;\n"
        "  PERFORM 2;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
    )

    class DummyConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            if not str(stmt).startswith("ALTER ROLE"):
                executed.append(stmt)

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            return 1

    class DummyEngine:
        def connect(self):
            return DummyConn()

    class DummyResource:
        def joinpath(self, name: str):
            return self

        def read_text(self, encoding: str = "utf-8") -> str:
            return sql

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db.resources, "files", lambda pkg: DummyResource())
    monkeypatch.setattr(db, "sqlparse", None)
    db.load_schema_statements.cache_clear()

    asyncio.run(db.apply_schema())
    assert len(executed) == 2
    assert executed[0].startswith("CREATE TABLE")
    assert executed[1].startswith("CREATE OR REPLACE FUNCTION")
    assert "PERFORM 2;" in executed[1]


def test_apply_schema_handles_tagged_dollar_quotes(monkeypatch):
    executed: list[str] = []

    sql = (
        "CREATE TABLE t(a int);\n"
        "CREATE OR REPLACE FUNCTION f() RETURNS void AS $func$\n"
        "BEGIN\n"
        "  EXECUTE $body$ SELECT 1; SELECT 2; $body$;\n"
        "  PERFORM 3;\n"
        "END;\n"
        "$func$ LANGUAGE plpgsql;\n"
    )

    class DummyConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            return 1

    class DummyEngine:
        def connect(self):
            return DummyConn()

    class DummyResource:
        def joinpath(self, name: str):
            return self

        def read_text(self, encoding: str = "utf-8") -> str:
            return sql

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db.resources, "files", lambda pkg: DummyResource())
    monkeypatch.setattr(db, "sqlparse", None)
    db.load_schema_statements.cache_clear()

    asyncio.run(db.apply_schema())

    assert executed[0].startswith("CREATE TABLE")
    assert executed[1].startswith("CREATE OR REPLACE FUNCTION")
    assert "$body$ SELECT 1; SELECT 2; $body$" in executed[1]


def test_create_database_noop_if_exists(monkeypatch):
    admin_urls: list[tuple[str, str | None]] = []
    executed: list[str] = []

    class AdminConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def scalar(self, stmt, params=None):
            return 1  # database exists

        async def execute(self, stmt, params=None):
            executed.append(stmt)

    class AdminEngine:
        def connect(self):
            return AdminConn()

        async def dispose(self):
            return None

    def fake_create_async_engine(url, echo=False, isolation_level=None):
        admin_urls.append((url, isolation_level))
        return AdminEngine()

    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(db, "text", lambda s: s)

    asyncio.run(db._create_database("postgresql+asyncpg://u@h/db"))

    assert admin_urls == [("postgresql+asyncpg://u@h/postgres", "AUTOCOMMIT")]
    assert executed == []


@pytest.mark.parametrize(
    "url",
    ["postgresql+asyncpg://u@h/db", "postgresql+psycopg://u@h/db"],
)
def test_create_database_executes_parameterized(monkeypatch, url):
    executed: list[tuple[str, dict[str, str] | None]] = []

    class AdminConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def scalar(self, stmt, params=None):
            return None  # database missing

        async def execute(self, stmt, params=None):
            executed.append((stmt, params))

    class AdminEngine:
        def connect(self):
            return AdminConn()

        async def dispose(self):
            return None

    def fake_create_async_engine(url, echo=False, isolation_level=None):
        return AdminEngine()

    monkeypatch.setattr(db, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(db, "text", lambda s: s)

    asyncio.run(db._create_database(url))

    assert executed == [("CREATE DATABASE \"db\"", None)]


def test_apply_schema_after_migrate_release_adult_partitions(monkeypatch):
    executed: list[str] = []

    class MigrationCursor:
        def execute(self, stmt, params=None):
            return None

        def fetchone(self):
            return (1,)

        def fetchall(self):
            return []

    class MigrationConn:
        def cursor(self):
            return MigrationCursor()

        def commit(self):
            return None

    migrate_release_adult_partitions(MigrationConn())

    class DummyConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            return 1

    class DummyEngine:
        def connect(self):
            return DummyConn()

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine())
    monkeypatch.setattr(db, "text", lambda s: s)

    asyncio.run(db.apply_schema())

    assert executed


def test_apply_schema_migrates_unpartitioned_release_adult(monkeypatch):
    executed: list[str] = []
    state = {"partitioned": False, "migrated": False}

    class DummyConn:
        def __init__(self, state):
            self.state = state

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            return self.state["partitioned"]

    class DummyEngine:
        def __init__(self, state):
            self.state = state

        def connect(self):
            return DummyConn(self.state)

        @property
        def sync_engine(self):
            return self

        def raw_connection(self):
            class RawConn:
                def close(self):
                    return None

            return RawConn()

    def fake_migrate(raw):
        state["migrated"] = True
        state["partitioned"] = True

    monkeypatch.setattr(db, "get_engine", lambda: DummyEngine(state))
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "migrate_release_adult_partitions", fake_migrate)

    asyncio.run(db.apply_schema())

    assert state["migrated"]
    assert executed
