import logging

from nzbidx_ingest import main
from nzbidx_ingest.main import connect_db


def test_connect_db_auto_migrate(monkeypatch, caplog):
    """connect_db should auto-migrate an unpartitioned release table."""

    called: dict[str, bool] = {"partitioned": False}

    class DummyCursor:
        def execute(self, *stmt):  # pragma: no cover - trivial
            sql = "".join(stmt)
            if "pg_class" in sql:
                self._result = (True,)
            elif "pg_partitioned_table" in sql:
                self._result = (called["partitioned"],)
            else:
                self._result = (False,)

        def fetchone(self):  # pragma: no cover - trivial
            return self._result

    class DummyRaw:
        def __init__(self) -> None:
            self._cursor = DummyCursor()

        def cursor(self):  # pragma: no cover - trivial
            return self._cursor

        def close(self):  # pragma: no cover - trivial
            pass

    class DummyConn(DummyRaw):
        def __enter__(self):  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            return False

        def execute(self, stmt):  # pragma: no cover - trivial
            return DummyResult(stmt)

        def commit(self):  # pragma: no cover - trivial
            pass

        def rollback(self):  # pragma: no cover - trivial
            pass

    class DummyResult:
        def __init__(self, stmt):
            self.stmt = stmt

        def fetchone(self):  # pragma: no cover - trivial
            if "pg_class" in self.stmt:
                return (True,)
            if "pg_partitioned_table" in self.stmt:
                return (True,)
            return (None,)

    class DummyEngine:
        def raw_connection(self):  # pragma: no cover - trivial
            return DummyRaw()

        def connect(self):  # pragma: no cover - trivial
            return DummyConn()

    def fake_migrate(conn):  # pragma: no cover - trivial
        called["partitioned"] = True

    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")
    monkeypatch.setenv("NZBIDX_AUTO_MIGRATE", "1")
    monkeypatch.setattr(main, "create_engine", lambda *a, **k: DummyEngine())
    monkeypatch.setattr(main, "text", lambda s: s)

    monkeypatch.setattr(main, "migrate_release_table", fake_migrate)

    with caplog.at_level(logging.INFO):
        conn = connect_db()
    assert called["partitioned"] is True
    assert conn is not None
    assert "release_table_migrating" in caplog.text
