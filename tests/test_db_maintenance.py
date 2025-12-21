import asyncio

from nzbidx_api import db


class DummyConn:
    def __init__(self):
        self.executed = []
        self.opts = None

    def execution_options(self, **opts):
        self.opts = opts
        return self

    async def execute(self, stmt):
        self.executed.append(stmt)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def run(coro):
    asyncio.run(coro)


def test_vacuum_analyze(monkeypatch):
    executed = []

    async def fake_maintenance(stmt: str):
        executed.append(stmt)

    async def fake_list(_conn):
        return ["public.release"]

    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(db, "_maintenance", fake_maintenance)
    monkeypatch.setattr(db, "_list_vacuum_tables", fake_list)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "_has_vacuum_privilege", lambda *_: True)

    run(db.vacuum_analyze())

    assert executed == ["VACUUM (ANALYZE) public.release"]


def test_reindex(monkeypatch):
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    run(db.reindex(table="release"))
    assert conn.executed == ["REINDEX TABLE release"]
    assert conn.opts == {"isolation_level": "AUTOCOMMIT"}


def test_analyze(monkeypatch):
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    run(db.analyze(table="release"))
    assert conn.executed == ["ANALYZE release"]
    assert conn.opts == {"isolation_level": "AUTOCOMMIT"}


def test_vacuum_analyze_skips_unprivileged_table(monkeypatch):
    executed = []

    async def fake_maintenance(stmt: str):
        executed.append(stmt)

    async def fake_has_privilege(*_args):
        return False

    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(db, "_maintenance", fake_maintenance)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "_has_vacuum_privilege", fake_has_privilege)

    run(db.vacuum_analyze(table="public.release"))

    assert executed == []


def test_list_vacuum_tables_filters_system_schemas(monkeypatch):
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "get_engine", lambda: engine)

    class DummyResult:
        def scalars(self):
            return self

        def all(self):
            return ["public.release"]

    async def fake_execute(stmt):
        conn.executed.append(stmt)
        return DummyResult()

    monkeypatch.setattr(conn, "execute", fake_execute)

    async def run_list():
        async with engine.connect() as c:
            return await db._list_vacuum_tables(c)

    result = asyncio.run(run_list())
    assert result == ["public.release"]
    assert any("NOT LIKE 'pg_%'" in stmt for stmt in conn.executed)
    assert any("has_table_privilege" in stmt for stmt in conn.executed)
