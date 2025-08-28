from __future__ import annotations

import importlib

import pytest


class FakeCursor:
    """Minimal cursor implementation for ``prune_sizes`` tests."""

    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.rowcount = 0
        self._rows: list[tuple[int | None]] = []

    def execute(self, query: str, params: tuple | None = None) -> None:
        if query.startswith("SELECT tablename FROM pg_tables"):
            self._rows = []
            return
        if query.startswith("SELECT DISTINCT category_id FROM"):
            cats = {row.get("category_id") for row in self.conn.rows}
            self._rows = [(c,) for c in cats]
            return
        if query.startswith("DELETE FROM release WHERE category_id") and params is not None:
            cat, limit = params
            removed: list[int] = []
            for idx, row in enumerate(self.conn.rows):
                if row.get("category_id") == cat and row.get("size_bytes", 0) < limit:
                    removed.append(idx)
            for idx in reversed(removed):
                del self.conn.rows[idx]
            self.rowcount = len(removed)
            return
        if query.startswith("DELETE FROM release WHERE size_bytes >") and params is not None:
            limit = params[0]
            removed = [i for i, r in enumerate(self.conn.rows) if r.get("size_bytes", 0) > limit]
            for idx in reversed(removed):
                del self.conn.rows[idx]
            self.rowcount = len(removed)

    def fetchall(self) -> list[tuple[int | None]]:  # pragma: no cover - trivial
        return self._rows


class FakeConnection:
    def __init__(self, rows: list[dict[str, int]]) -> None:
        self.rows = rows

    def cursor(self) -> FakeCursor:  # pragma: no cover - trivial
        return FakeCursor(self)

    def commit(self) -> None:  # pragma: no cover - trivial
        pass

    def close(self) -> None:  # pragma: no cover - trivial
        pass


def test_prune_disallowed_sizes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TV_MIN_SIZE", "100")
    monkeypatch.setenv("AUDIO_MIN_SIZE", "50")
    monkeypatch.setenv("MAX_RELEASE_BYTES", "0")

    import nzbidx_ingest.config as cfg

    importlib.reload(cfg)

    import scripts.prune_disallowed_sizes as mod

    importlib.reload(mod)

    conn = FakeConnection(
        [
            {"category_id": 5000, "size_bytes": 90},
            {"category_id": 5000, "size_bytes": 110},
            {"category_id": 3000, "size_bytes": 40},
            {"category_id": 3000, "size_bytes": 60},
        ]
    )

    monkeypatch.setattr(mod, "connect_db", lambda: conn)
    monkeypatch.setattr(mod, "sql_placeholder", lambda conn: "?")

    deleted = mod.prune_sizes()
    assert deleted == 2
    remaining = sorted((r["category_id"], r["size_bytes"]) for r in conn.rows)
    assert remaining == [(3000, 60), (5000, 110)]

