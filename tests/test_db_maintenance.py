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
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    run(db.vacuum_analyze())
    assert conn.executed == ["VACUUM (ANALYZE)"]
    assert conn.opts == {"isolation_level": "AUTOCOMMIT"}


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
