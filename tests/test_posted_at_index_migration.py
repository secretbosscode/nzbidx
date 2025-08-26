from __future__ import annotations

import asyncio
import importlib
from typing import List, Sequence, Tuple

from sqlalchemy.ext.asyncio import create_async_engine

from nzbidx_api import db

m_posted = importlib.import_module("nzbidx_api.migrations.0001_add_posted_at_index")


class FeatureNotSupportedError(Exception):
    """Simulate PostgreSQL's FeatureNotSupportedError."""


class FakeCursor:
    def __init__(
        self, executed: List[str], partitions: Sequence[Tuple[str, bool]]
    ) -> None:
        self.executed = executed
        self.partitions = partitions
        self._fetch: Sequence[Tuple[str, bool]] = []
        self._partitioned = {t for t, is_leaf in partitions if not is_leaf}

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if "FROM pg_partition_tree" in sql:
            self._fetch = self.partitions
        elif sql.startswith("CREATE INDEX CONCURRENTLY") and any(
            f'ON "{t}"' in sql for t in self._partitioned
        ):
            raise FeatureNotSupportedError(
                "cannot create index concurrently on partitioned table",
            )

    def fetchall(self) -> Sequence[Tuple[str, bool]]:
        return self._fetch

    def close(self) -> None:  # pragma: no cover - included for interface completeness
        pass


class FakeConn:
    def __init__(
        self, executed: List[str], partitions: Sequence[Tuple[str, bool]]
    ) -> None:
        self.executed = executed
        self.partitions = partitions

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.executed, self.partitions)


def test_migrate_handles_partitioned_parent() -> None:
    partitions = [("release_child", True), ("release", False)]
    executed: List[str] = []
    conn = FakeConn(executed, partitions)
    m_posted.migrate(conn)
    assert (
        'CREATE INDEX IF NOT EXISTS "release_posted_at_idx" ON "release" (posted_at)'
        in executed
    )
    assert (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "release_child_posted_at_idx" ON "release_child" (posted_at)'
        in executed
    )


def test_migrate_handles_partitioned_partition() -> None:
    partitions = [
        ("release_adult_yes", True),
        ("release_adult_no", True),
        ("release_adult", False),
        ("release_child", True),
        ("release", False),
    ]
    executed: List[str] = []
    conn = FakeConn(executed, partitions)
    m_posted.migrate(conn)
    assert (
        'CREATE INDEX IF NOT EXISTS "release_adult_posted_at_idx" ON "release_adult" (posted_at)'
        in executed
    )
    assert (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "release_adult_yes_posted_at_idx" ON "release_adult_yes" (posted_at)'
        in executed
    )
    assert (
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "release_adult_no_posted_at_idx" ON "release_adult_no" (posted_at)'
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
    db.load_schema_statements.cache_clear()

    orig_migrate = m_posted.migrate

    def wrapper(_conn):
        executed: List[str] = []
        partitions = [("release_child", True), ("release", False)]
        fake_conn = FakeConn(executed, partitions)
        orig_migrate(fake_conn)

    monkeypatch.setattr(m_posted, "migrate", wrapper)

    asyncio.run(db.apply_schema())
    asyncio.run(engine.dispose())
