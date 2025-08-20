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
                return types.SimpleNamespace(fetchone=lambda: (storage.get(params[0]),))
            return types.SimpleNamespace(fetchone=lambda: None)

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


def test_concurrent_backends_isolated(monkeypatch):
    import threading
    import nzbidx_ingest.cursors as curs

    barrier = threading.Barrier(2)
    tl = threading.local()
    executed: dict[str, str] = {}

    class DummyCursor:
        def fetchone(self):
            return None

        def fetchall(self):  # pragma: no cover - not used
            return []

    class DummyConn:
        def __init__(self, style: str):
            self.style = style

        def execute(self, stmt: str, params=None):
            barrier.wait()
            executed[self.style] = stmt
            return DummyCursor()

        def commit(self):  # pragma: no cover - trivial
            return None

        def close(self):  # pragma: no cover - trivial
            return None

    def fake_conn():
        style = tl.style
        return DummyConn(style), style

    monkeypatch.setattr(curs, "_conn", fake_conn)

    def run(style: str, fn, *args):
        tl.style = style
        fn(*args)

    t1 = threading.Thread(target=run, args=("?", curs.get_cursor, "g1"))
    t2 = threading.Thread(target=run, args=("%s", curs.set_cursor, "g2", 1))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert executed["?"].count("?") > 0
    assert executed["%s"].count("%s") > 0
