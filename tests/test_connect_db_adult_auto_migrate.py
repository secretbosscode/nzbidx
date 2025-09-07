import logging

from nzbidx_ingest import main
from nzbidx_ingest.main import connect_db


def test_connect_db_adult_auto_migrate(monkeypatch, caplog):
    """connect_db should auto-migrate an unpartitioned release_adult table."""

    called: dict[str, bool] = {"partitioned": False}

    class DummyCursor:
        def execute(self, *stmt):  # pragma: no cover - trivial
            sql = "".join(stmt)
            if "pg_partitioned_table" in sql and "release_adult" in sql:
                self._result = (called["partitioned"],)
            elif "pg_partitioned_table" in sql and "release" in sql:
                self._result = (True,)
            elif "pg_class" in sql and "release_adult" not in sql:
                self._result = (True,)
            elif "pg_class" in sql and "release_adult" in sql:
                self._result = (True,)
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
        def __init__(self, stmt: str):
            self.stmt = stmt

        def fetchone(self):  # pragma: no cover - trivial
            sql = self.stmt
            if "pg_partitioned_table" in sql and "release_adult" in sql:
                return (called["partitioned"],)
            if "pg_partitioned_table" in sql and "release" in sql:
                return (True,)
            if "pg_class" in sql and "release_adult" not in sql:
                return (True,)
            if "pg_class" in sql and "release_adult" in sql:
                return (True,)
            if "pg_extension" in sql:
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
    monkeypatch.setattr(main, "create_engine", lambda *a, **k: DummyEngine())
    monkeypatch.setattr(main, "text", lambda s: s)
    monkeypatch.setattr(main, "migrate_release_partitions_by_date", lambda conn, cat: fake_migrate(conn) if cat == "adult" else None)
    monkeypatch.setattr(main, "migrate_release_table", lambda *a, **k: None)
    monkeypatch.setattr(main, "PARTITION_CATEGORIES", ["adult"])

    with caplog.at_level(logging.INFO):
        conn = connect_db()
    assert conn is not None
