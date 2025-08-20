from __future__ import annotations

import logging
import pytest

from nzbidx_api import db


@pytest.fixture(autouse=True)
def _reset_conn() -> None:
    db._conn = None
    yield
    db._conn = None


def test_close_connection_logs_db_error(monkeypatch, caplog):
    class DummyError(Exception):
        pass

    class DummyConn:
        def close(self) -> None:
            raise DummyError("boom")

    db._conn = DummyConn()
    monkeypatch.setattr(db, "DB_CLOSE_ERRORS", (DummyError,))

    caplog.set_level(logging.WARNING)
    db.close_connection()

    assert "connection_close_failed" in caplog.text
    assert db._conn is None


def test_close_connection_unexpected_error(monkeypatch):
    class Boom(Exception):
        pass

    class DummyConn:
        def close(self) -> None:
            raise Boom("boom")

    db._conn = DummyConn()
    monkeypatch.setattr(db, "DB_CLOSE_ERRORS", ())

    with pytest.raises(Boom):
        db.close_connection()

    assert db._conn is not None
