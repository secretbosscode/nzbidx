import sqlite3

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore
from nzbidx_api.json_utils import orjson


def test_ingest_handles_surrogates(monkeypatch, tmp_path) -> None:
    # Configure environment and cursors
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [
                {
                    ":bytes": "100",
                    "subject": "Example\udce2(1/1)",
                    "message-id": "<m1\udce2>",
                }
            ]

        def body_size(self, _mid: str) -> int:
            return 100

    monkeypatch.setattr(loop, "NNTPClient", lambda _settings: DummyClient())

    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, segments TEXT, UNIQUE (norm_title, category_id, posted_at))"
        )
        # Pre-insert row to be updated
        conn.execute(
            "INSERT INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, segments) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("example", "other", 7000, "und", "", "alt.test", 0, "[]"),
        )
        conn.commit()
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)
    monkeypatch.setattr(
        loop, "insert_release", lambda _db, releases: {r[0] for r in releases}
    )

    # Should not raise despite surrogate characters
    loop.run_once()

    with sqlite3.connect(db_path) as check:
        row = check.execute(
            "SELECT segments FROM release WHERE norm_title = 'example'"
        ).fetchone()
    assert row is not None
    segments = orjson.loads(row[0])
    assert segments == [
        {"number": 1, "message_id": "m1", "group": "alt.test", "size": 100}
    ]


def test_ingest_cleans_existing_segments(monkeypatch, tmp_path) -> None:
    # Configure environment and cursors
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 2

        def xover(self, group: str, start: int, end: int):
            return [
                {
                    ":bytes": "100",
                    "subject": "Example\udce2(2/2)",
                    "message-id": "<m2>",
                }
            ]

        def body_size(self, _mid: str) -> int:
            return 100

    monkeypatch.setattr(loop, "NNTPClient", lambda _settings: DummyClient())

    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, segments TEXT, UNIQUE (norm_title, category_id, posted_at))"
        )
        existing_segments = '[{"number":1,"message_id":"m1\\udce2","group":"alt.test\\udce2","size":100}]'
        conn.execute(
            "INSERT INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, segments) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("example", "other", 7000, "und", "", "alt.test", 100, existing_segments),
        )
        conn.commit()
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)
    monkeypatch.setattr(
        loop, "insert_release", lambda _db, releases: {r[0] for r in releases}
    )

    # Should not raise despite surrogate characters in existing segments
    loop.run_once()

    with sqlite3.connect(db_path) as check:
        row = check.execute(
            "SELECT segments FROM release WHERE norm_title = 'example'"
        ).fetchone()
    assert row is not None
    segments = orjson.loads(row[0])
    assert segments == [
        {"number": 1, "message_id": "m1", "group": "alt.test", "size": 100},
        {"number": 2, "message_id": "m2", "group": "alt.test", "size": 100},
    ]
