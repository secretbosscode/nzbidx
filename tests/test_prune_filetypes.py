from __future__ import annotations

import os

import pytest

from nzbidx_ingest.main import prune_disallowed_filetypes


class FakeCursor:
    """Minimal cursor implementation for ``prune_disallowed_filetypes`` tests."""

    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.rowcount = 0
        self._rows: list[tuple[str]] = []

    def __enter__(self) -> "FakeCursor":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        pass

    def execute(self, query: str, params: tuple | None = None) -> None:
        if query.startswith("SELECT tablename FROM pg_tables"):
            # No partition tables in tests.
            self._rows = []
            return
        if query.startswith("DELETE FROM release") and params is not None:
            allowed = set(params[:-1])
            limit = params[-1]
            removed: list[int] = []
            for idx, row in enumerate(self.conn.rows):
                ext = row.get("extension")
                if ext is not None and ext.lower() not in allowed:
                    removed.append(idx)
                    if len(removed) >= limit:
                        break
            for idx in reversed(removed):
                del self.conn.rows[idx]
            self.rowcount = len(removed)

    def fetchall(self) -> list[tuple[str]]:  # pragma: no cover - trivial
        return self._rows


class FakeConnection:
    def __init__(self, rows: list[dict[str, str | None]]) -> None:
        self.rows = rows

    def cursor(self) -> FakeCursor:  # pragma: no cover - trivial
        return FakeCursor(self)

    def commit(self) -> None:  # pragma: no cover - trivial
        pass


def _clear_file_extension_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ.keys()):
        if key.startswith("FILE_EXTENSIONS_"):
            monkeypatch.delenv(key, raising=False)


def test_prune_disallowed_filetypes_default(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_file_extension_env(monkeypatch)
    conn = FakeConnection(
        [
            {"extension": "rar"},
            {"extension": "foo"},
        ]
    )
    deleted = prune_disallowed_filetypes(conn)
    assert deleted == 1
    assert [r["extension"] for r in conn.rows] == ["rar"]


def test_prune_disallowed_filetypes_env_extends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_file_extension_env(monkeypatch)
    monkeypatch.setenv("FILE_EXTENSIONS_EXTRA", "foo")
    conn = FakeConnection(
        [
            {"extension": "rar"},
            {"extension": "foo"},
        ]
    )
    deleted = prune_disallowed_filetypes(conn)
    assert deleted == 0
    assert [r["extension"] for r in conn.rows] == ["rar", "foo"]


@pytest.mark.parametrize("ext", ["jpg", "jpeg", "png", "gif"])
def test_prune_disallowed_filetypes_images(
    ext: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_file_extension_env(monkeypatch)
    conn = FakeConnection(
        [
            {"extension": ext},
        ]
    )
    deleted = prune_disallowed_filetypes(conn)
    assert deleted == 1
    assert conn.rows == []
