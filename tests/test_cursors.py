from __future__ import annotations

import importlib
import sys
import types
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

        def executemany(self, stmt: str, param_list):  # type: ignore[override]
            for params in param_list:
                self.execute(stmt, params)

        def commit(self) -> None:  # pragma: no cover - trivial
            return None

        def close(self) -> None:  # pragma: no cover - trivial
            return None

    def fake_connect(url: str) -> DummyConn:
        executed.append(("url", url))
        return DummyConn()

    fake_psycopg = types.SimpleNamespace(connect=fake_connect)
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


def test_concurrent_backends_isolated(tmp_path, monkeypatch):
    import importlib
    import nzbidx_ingest.cursors as curs
    import nzbidx_ingest.config as cfg

    monkeypatch.setenv("CURSOR_DB", str(tmp_path / "conn.sqlite"))
    importlib.reload(cfg)
    importlib.reload(curs)

    calls: list[None] = []
    orig = curs._get_conn

    def counting_get_conn():
        if curs._CONN is None:
            calls.append(None)
        return orig()

    monkeypatch.setattr(curs, "_get_conn", counting_get_conn)

    curs.get_cursor("g1")
    curs.set_cursor("g1", 1)

    assert len(calls) == 1
