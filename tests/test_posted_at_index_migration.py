from __future__ import annotations

import asyncio
import importlib
from typing import List

from sqlalchemy.ext.asyncio import create_async_engine

from nzbidx_api import db

m_posted = importlib.import_module("nzbidx_api.migrations.0001_add_posted_at_index")
m_search = importlib.import_module("nzbidx_api.migrations.0001_add_search_vector")


class FeatureNotSupportedError(Exception):
    """Simulate PostgreSQL's FeatureNotSupportedError."""


class FakeCursor:
    def __init__(self, executed: List[str]) -> None:
        self.executed = executed
        self._fetch: List[tuple[str]] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if "FROM pg_inherits" in sql:
            self._fetch = [("release_child",)]
        elif sql.startswith("CREATE INDEX CONCURRENTLY") and 'ON "release"' in sql:
            raise FeatureNotSupportedError(
                "cannot create index concurrently on partitioned table"
            )

    def fetchall(self) -> List[tuple[str]]:
        return self._fetch

    def close(self) -> None:  # pragma: no cover - included for interface completeness
        pass


class FakeConn:
    def __init__(self, executed: List[str]) -> None:
        self.executed = executed

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.executed)


def test_migrate_handles_partitioned_parent() -> None:
    executed: List[str] = []
    conn = FakeConn(executed)
    m_posted.migrate(conn)
    assert (
        'CREATE INDEX IF NOT EXISTS "release_posted_at_idx" ON "release" (posted_at)'
        in executed
    )
    assert (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "release_child_posted_at_idx" ON "release_child" (posted_at)'
        in executed
    )


def test_apply_schema_runs_migration_without_feature_not_supported(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setattr(db, "get_engine", lambda: engine)

    class DummyResource:
        def joinpath(self, name: str):
            return self

        def read_text(self, encoding: str = "utf-8") -> str:
            return "CREATE TABLE release (id INTEGER PRIMARY KEY, posted_at TIMESTAMP);"

    monkeypatch.setattr(db.resources, "files", lambda pkg: DummyResource())

    orig_migrate = m_posted.migrate

    def wrapper(_conn):
        executed: List[str] = []
        fake_conn = FakeConn(executed)
        orig_migrate(fake_conn)

    monkeypatch.setattr(m_posted, "migrate", wrapper)
    monkeypatch.setattr(m_search, "migrate", lambda _conn: None)

    asyncio.run(db.apply_schema())
    asyncio.run(engine.dispose())
