from nzbidx_ingest import main
from nzbidx_ingest.main import connect_db


def test_connect_db_migrates_legacy_table(monkeypatch):
    """connect_db should migrate an existing non-partitioned table."""

    class DummyCursor:
        def __init__(self, raw):
            self.raw = raw

        def execute(self, stmt, *args, **kwargs):  # pragma: no cover - trivial
            if "FROM pg_class" in stmt:
                self._result = (True,)  # table exists
            elif "pg_partitioned_table" in stmt:
                self._result = (self.raw.migrated,)
            else:
                self._result = (None,)

        def fetchone(self):  # pragma: no cover - trivial
            return self._result

    class DummyRaw:
        def __init__(self):
            self.migrated = False
            self._cursor = DummyCursor(self)

        def cursor(self):  # pragma: no cover - trivial
            return self._cursor

        def commit(self):  # pragma: no cover - trivial
            pass

    class DummyResult:
        def fetchone(self):  # pragma: no cover - trivial
            return (True,)

    class DummyConn:
        def __enter__(self):  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            pass

        def execute(self, stmt):  # pragma: no cover - trivial
            return DummyResult()

        def commit(self):  # pragma: no cover - trivial
            pass

        def rollback(self):  # pragma: no cover - trivial
            pass

    class DummyEngine:
        def __init__(self):
            self.raw = DummyRaw()

        def raw_connection(self):  # pragma: no cover - trivial
            return self.raw

        def connect(self):  # pragma: no cover - trivial
            return DummyConn()

    called = False

    def dummy_migrate(conn):  # pragma: no cover - trivial
        nonlocal called
        called = True
        conn.migrated = True

    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")
    monkeypatch.setattr(main, "create_engine", lambda *a, **k: DummyEngine())
    monkeypatch.setattr(main, "text", lambda s: s)
    monkeypatch.setattr(main, "migrate_release_table", dummy_migrate)

    if hasattr(main, "_SCHEMA_CHECKED"):
        main._SCHEMA_CHECKED.clear()
    connect_db()
    assert called
