from __future__ import annotations

import sqlite3

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore
from nzbidx_ingest.main import CATEGORY_MAP  # type: ignore


def test_ingested_releases_include_size(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "get_cursors", lambda gs: {g: 0 for g in gs})
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "set_cursors", lambda _u: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{"subject": "Example", ":bytes": "456"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, UNIQUE (norm_title, category_id))"
        )
        return conn

    with _connect() as conn:
        loop.run_once(conn)

    with sqlite3.connect(db_path) as check:
        row = check.execute("SELECT norm_title, size_bytes FROM release").fetchone()
    assert row == ("example", 456)


def test_multi_part_release_size_summed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "get_cursors", lambda gs: {g: 0 for g in gs})
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "set_cursors", lambda _u: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 2

        def xover(self, group: str, start: int, end: int):
            return [
                {"subject": "Example (1/2)", ":bytes": "100"},
                {"subject": "Example (2/2)", ":bytes": "200"},
            ]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, UNIQUE (norm_title, category_id))"
        )
        return conn

    with _connect() as conn:
        loop.run_once(conn)

    with sqlite3.connect(db_path) as check:
        row = check.execute("SELECT norm_title, size_bytes FROM release").fetchone()
    assert row == ("example", 300)


def test_zero_byte_release_skipped(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "get_cursors", lambda gs: {g: 0 for g in gs})
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "set_cursors", lambda _u: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{"subject": "Example", ":bytes": "0"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, UNIQUE (norm_title, category_id))"
        )
        return conn

    with _connect() as conn:
        loop.run_once(conn)

    with sqlite3.connect(db_path) as check:
        row = check.execute("SELECT * FROM release").fetchone()
    assert row is None


def test_same_title_different_groups(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        config, "NNTP_GROUPS", ["alt.movies", "alt.music"], raising=False
    )
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "get_cursors", lambda gs: {g: 0 for g in gs})
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "set_cursors", lambda _u: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{"subject": "Example", ":bytes": "100"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, UNIQUE(norm_title, category_id))"
        )
        return conn

    with _connect() as conn:
        loop.run_once(conn)

    with sqlite3.connect(db_path) as check:
        rows = check.execute(
            "SELECT norm_title, category_id FROM release ORDER BY category_id"
        ).fetchall()
    assert rows == [
        ("example", int(CATEGORY_MAP["movies"])),
        ("example", int(CATEGORY_MAP["audio"])),
    ]
