from __future__ import annotations

import importlib
import sys
import types
import logging
import sqlite3

import pytest

import nzbidx_ingest.cursors as cursors
import nzbidx_ingest.config as config


def test_cursor_db_path_created(tmp_path, monkeypatch):
    db_path = tmp_path / "subdir" / "cursors.sqlite"
    monkeypatch.setenv("CURSOR_DB", str(db_path))
    importlib.reload(config)
    importlib.reload(cursors)

    cursors.set_cursor("alt.example", 42)
    assert cursors.get_cursor("alt.example") == 42
    assert db_path.exists()


def test_cursor_postgres_dsn(monkeypatch):
    executed: list[tuple[str, object | None]] = []
    storage: dict[str, int] = {}

    class DummyConn:
        def execute(self, stmt: str, params: tuple | None = None):
            executed.append((stmt, params))
            if stmt.startswith("INSERT") and params:
                storage[params[0]] = params[1]
            if stmt.startswith("SELECT") and params:
                value = storage.get(params[0])
                return types.SimpleNamespace(
                    fetchone=lambda: (value,),
                    fetchall=lambda: [(params[0], value)] if value is not None else [],
                )
            return types.SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])

        def cursor(self):  # pragma: no cover - trivial
            conn = self

            class DummyCursor:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(
                    self_inner, exc_type, exc, tb
                ):  # pragma: no cover - trivial
                    return False

                def executemany(self_inner, stmt: str, param_list):
                    for params in param_list:
                        conn.execute(stmt, params)

            return DummyCursor()

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def close(self) -> None:  # pragma: no cover - trivial
            return None

    def fake_connect(url: str) -> DummyConn:
        executed.append(("url", url))
        return DummyConn()

    fake_psycopg = types.SimpleNamespace(
        connect=fake_connect,
        Error=Exception,
        errors=types.SimpleNamespace(DuplicateColumn=Exception),
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setenv("CURSOR_DB", "postgres://user@host/db")
    importlib.reload(config)
    importlib.reload(cursors)

    cursors.set_cursor("alt.example", 99)
    assert cursors.get_cursor("alt.example") == 99
    assert executed[0] == ("url", "postgresql://user@host/db")
    insert_stmt = next(stmt for stmt, _ in executed if stmt.startswith("INSERT"))
    select_stmt = next(stmt for stmt, _ in executed if stmt.startswith("SELECT"))
    assert "%s" in insert_stmt and "%s" in select_stmt


def test_bulk_cursor_helpers(tmp_path, monkeypatch):
    db_path = tmp_path / "bulk.sqlite"
    monkeypatch.setenv("CURSOR_DB", str(db_path))
    importlib.reload(config)
    importlib.reload(cursors)

    cursors.set_cursors({"g1": 1, "g2": 2})
    result = cursors.get_cursors(["g1", "g2", "g3"])
    assert result == {"g1": 1, "g2": 2}


def test_conn_sqlite_path_no_name_error(tmp_path, monkeypatch):
    db_path = tmp_path / "conn.sqlite"
    monkeypatch.setenv("CURSOR_DB", str(db_path))
    importlib.reload(config)
    importlib.reload(cursors)
    try:
        conn, _ = cursors._conn()
    except NameError as e:
        pytest.fail(f"_conn raised NameError: {e}")
    else:
        conn.close()


def test_set_cursors_logs_error_on_failure(monkeypatch, caplog):
    calls: dict[str, bool] = {"closed": False, "rollback": False}

    class DummyConn:
        class DummyCursor:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):  # pragma: no cover - trivial
                return False

            def executemany(self_inner, stmt, data):
                raise sqlite3.OperationalError("locked")

        def cursor(self):
            return self.DummyCursor()

        def commit(self) -> None:  # pragma: no cover - not reached
            pass

        def rollback(self) -> None:
            calls["rollback"] = True

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr(cursors, "_conn", lambda: (DummyConn(), "?"))

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            cursors.set_cursors({"g": 1})

    assert calls["rollback"] and calls["closed"]
    assert "cursor_update_failed" in caplog.text


def test_conn_multiple_calls_no_duplicate_sqlite(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dup.sqlite"
    monkeypatch.setenv("CURSOR_DB", str(db_path))
    importlib.reload(config)
    importlib.reload(cursors)
    with caplog.at_level(logging.ERROR):
        conn1, _ = cursors._conn()
        conn1.close()
        conn2, _ = cursors._conn()
        conn2.close()
    assert not caplog.records


def test_conn_multiple_calls_no_duplicate_postgres(monkeypatch, caplog):
    executed: list[str] = []

    class DummyDuplicateColumn(Exception):
        pass

    class DummyConn:
        def execute(self, stmt: str, params: tuple | None = None):
            executed.append(stmt)
            if stmt == "ALTER TABLE cursor ADD COLUMN irrelevant INTEGER DEFAULT 0":
                raise DummyDuplicateColumn("duplicate column")
            return types.SimpleNamespace(fetchall=lambda: [])

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def rollback(self) -> None:  # pragma: no cover - trivial
            return None

        def close(self) -> None:  # pragma: no cover - trivial
            return None

    fake_psycopg = types.SimpleNamespace(
        connect=lambda url: DummyConn(),
        errors=types.SimpleNamespace(DuplicateColumn=DummyDuplicateColumn),
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setenv("CURSOR_DB", "postgres://user@host/db")
    importlib.reload(config)
    importlib.reload(cursors)
    with caplog.at_level(logging.ERROR):
        conn1, _ = cursors._conn()
        conn1.close()
        conn2, _ = cursors._conn()
        conn2.close()
    assert not caplog.records
    alter_stmts = [stmt for stmt in executed if stmt.startswith("ALTER TABLE cursor")]
    assert all("IF NOT EXISTS" in stmt for stmt in alter_stmts)
