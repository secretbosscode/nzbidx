from __future__ import annotations

import importlib
from typing import List

m_search = importlib.import_module("nzbidx_api.migrations.0001_add_search_vector")


class FeatureNotSupportedError(Exception):
    """Simulate PostgreSQL's FeatureNotSupportedError."""


class FakeCursor:
    def __init__(self, executed: List[str]) -> None:
        self.executed = executed

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if "CONCURRENTLY" in sql:
            raise FeatureNotSupportedError(
                "cannot create index concurrently on partitioned table"
            )

    def close(self) -> None:  # pragma: no cover - interface completeness
        pass


class FakeConn:
    def __init__(self, executed: List[str]) -> None:
        self.executed = executed

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.executed)


def test_migrate_handles_partitioned_release() -> None:
    executed: List[str] = []
    conn = FakeConn(executed)
    m_search.migrate(conn)
    assert any(
        "CREATE INDEX IF NOT EXISTS release_search_idx" in sql
        and "USING GIN (search_vector)" in sql
        for sql in executed
    )
