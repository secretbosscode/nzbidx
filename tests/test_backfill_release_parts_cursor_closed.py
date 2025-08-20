from __future__ import annotations

import pytest

from nzbidx_api import backfill_release_parts as backfill_mod


class DummyCursor:
    def __init__(self) -> None:
        self.closed = False

    def __enter__(self) -> "DummyCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:  # pragma: no cover - trivial
        self.closed = True

    def execute(self, stmt: str, params=None) -> None:  # pragma: no cover - trivial
        return None

    def fetchmany(self, size: int):
        raise RuntimeError("boom")


class DummyConn:
    def __init__(self) -> None:
        self.cursor_obj = DummyCursor()

    def cursor(self) -> DummyCursor:
        return self.cursor_obj

    def execute(self, stmt: str, params=None) -> None:  # pragma: no cover - trivial
        return None

    def commit(self) -> None:  # pragma: no cover - trivial
        return None

    def close(self) -> None:  # pragma: no cover - trivial
        return None


def test_cursor_closed_on_exception(monkeypatch):
    conn = DummyConn()
    monkeypatch.setattr(backfill_mod, "connect_db", lambda: conn)
    with pytest.raises(RuntimeError):
        backfill_mod.backfill_release_parts()
    assert conn.cursor_obj.closed
