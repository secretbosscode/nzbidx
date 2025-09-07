import asyncio

from nzbidx_api import db


async def _run_apply_schema() -> list[str]:
    executed: list[str] = []
    state = {"partitioned": False, "migrated": False}

    class DummyCursor:
        def __init__(self, state):
            self.state = state
            self.sql = ""

        def execute(self, stmt, params=None):  # pragma: no cover - trivial
            self.sql = stmt

        def fetchone(self):  # pragma: no cover - trivial
            if "pg_partitioned_table" in self.sql:
                return (self.state["partitioned"],)
            if "pg_class" in self.sql:
                return (True,)
            return (None,)

        def fetchall(self):  # pragma: no cover - trivial
            return []

    class RawConn:
        def __init__(self, state):
            self.state = state

        def cursor(self):  # pragma: no cover - trivial
            return DummyCursor(self.state)

        def commit(self):  # pragma: no cover - trivial
            return None

    class DummySyncConn:
        def __init__(self, state):
            self.connection = type("C", (), {"dbapi_connection": RawConn(state)})()

    class DummyConn:
        def __init__(self, state):
            self.state = state

        async def __aenter__(self):  # pragma: no cover - trivial
            return self

        async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            return None

        async def execute(self, stmt, params=None):
            executed.append(stmt)

        async def commit(self):  # pragma: no cover - trivial
            return None

        async def rollback(self):  # pragma: no cover - trivial
            return None

        async def scalar(self, stmt, params=None):  # pragma: no cover - trivial
            return self.state["partitioned"]

        async def run_sync(self, fn):  # pragma: no cover - trivial
            result = fn(DummySyncConn(self.state))
            if asyncio.iscoroutine(result):
                await result

    class DummyEngine:
        def __init__(self, state):
            self.state = state
            self.url = "postgresql://"

        def connect(self):  # pragma: no cover - trivial
            return DummyConn(self.state)

        @property
        def sync_engine(self):  # pragma: no cover - trivial
            return self

        def raw_connection(self):  # pragma: no cover - trivial
            return RawConn(self.state)

    def fake_migrate(raw, category):
        executed.append(f"MIGRATE {category}")
        state["partitioned"] = True
        state["migrated"] = True

    db.get_engine = lambda: DummyEngine(state)  # type: ignore[attr-defined]
    db.text = lambda s: s  # type: ignore[attr-defined]
    db.migrate_release_partitions_by_date = fake_migrate  # type: ignore[attr-defined]
    db.CATEGORY_RANGES = {"movies": (2000, 3000)}  # type: ignore[attr-defined]
    db.load_schema_statements = (  # type: ignore[attr-defined]
        lambda: [
            "CREATE TABLE IF NOT EXISTS release_movies PARTITION OF release FOR VALUES FROM (2000) TO (3000) PARTITION BY RANGE (posted_at)",
            "CREATE TABLE IF NOT EXISTS release_movies_2024 PARTITION OF release_movies FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        ]
    )

    await db.apply_schema()

    assert state["migrated"]
    return executed


def test_apply_schema_partitions_release_movies():
    executed = asyncio.run(_run_apply_schema())
    assert any("release_movies_2024" in stmt for stmt in executed)
