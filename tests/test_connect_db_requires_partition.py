import logging

import pytest

from nzbidx_ingest import main
from nzbidx_ingest.main import connect_db


def test_connect_db_requires_partition(monkeypatch, caplog):
    """connect_db should fail if release table is not partitioned."""

    class DummyCursor:
        def execute(self, stmt):  # pragma: no cover - trivial
            # Simulate existing table but missing partition metadata
            if "pg_class" in stmt:
                self._result = (True,)
            elif "pg_partitioned_table" in stmt:
                self._result = (False,)
            else:
                self._result = (False,)

        def fetchone(self):  # pragma: no cover - trivial
            return self._result

    class DummyRaw:
        def __init__(self) -> None:
            self._cursor = DummyCursor()

        def cursor(self):  # pragma: no cover - trivial
            return self._cursor

    class DummyEngine:
        def raw_connection(self):  # pragma: no cover - trivial
            return DummyRaw()

        def connect(self):  # pragma: no cover - should not be called
            raise AssertionError("connect should not be called")

    monkeypatch.setenv("DATABASE_URL", "postgres://user@host/db")
    monkeypatch.setattr(main, "create_engine", lambda *a, **k: DummyEngine())
    monkeypatch.setattr(main, "text", lambda s: s)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="partitioned"):
            connect_db()
    assert "release_table_not_partitioned" in caplog.text
