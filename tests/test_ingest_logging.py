from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import json
import logging
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore


def test_ingest_batch_log(monkeypatch, caplog) -> None:
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
            return [{":bytes": "100", "subject": "Example"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(
        loop, "insert_release", lambda _db, releases: {r[0] for r in releases}
    )

    with caplog.at_level(logging.INFO):
        loop.run_once()

    record = next(r for r in caplog.records if r.message.startswith("Processed"))
    assert "Processed 1 items (inserted 1, deduplicated 0)." in record.message
    assert not hasattr(record, "avg_batch_ms")
    assert not hasattr(record, "os_latency_ms")
    assert not hasattr(record, "avg_db_ms")
    assert not hasattr(record, "avg_os_ms")
    assert not hasattr(record, "pct_complete")
    assert not hasattr(record, "eta_s")
    assert not hasattr(record, "deduped")


def test_existing_release_reindexed_with_new_segments(monkeypatch, tmp_path) -> None:
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
            return [{"subject": "Example (2/2)", ":bytes": "150", "message-id": "<m2>"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, segments TEXT)"
        )
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)

    with _connect() as conn:
        conn.execute(
            "INSERT INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, has_parts, part_count, segments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "example",
                "other",
                7000,
                "und",
                "",
                "alt.test",
                100,
                1,
                1,
                json.dumps([(1, "m1", "alt.test", 100)]),
            ),
        )
        conn.commit()

    monkeypatch.setattr(loop, "insert_release", lambda _db, releases: set())

    loop.run_once()

    with sqlite3.connect(db_path) as check:
        row = check.execute(
            "SELECT size_bytes, part_count, segments FROM release WHERE norm_title = 'example'",
        ).fetchone()
    assert row[0] == 250
    assert row[1] == 2
    assert json.loads(row[2]) == [
        [1, "m1", "alt.test", 100],
        [2, "m2", "alt.test", 150],
    ]


def test_duplicate_segments_do_not_set_has_parts(monkeypatch, tmp_path) -> None:
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
            return [{"subject": "Example (1/1)", ":bytes": "100", "message-id": "<m1>"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, segments TEXT)"
        )
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)

    with _connect() as conn:
        conn.execute(
            "INSERT INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, has_parts, part_count, segments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "example",
                "other",
                7000,
                "und",
                "",
                "alt.test",
                100,
                0,
                0,
                json.dumps([]),
            ),
        )
        conn.commit()

    monkeypatch.setattr(loop, "insert_release", lambda _db, releases: set())

    class _Existing(list):
        def __init__(self) -> None:
            super().__init__([(1, "m1", "alt.test", 100)])

        def __add__(self, _other):
            return []

    def fake_loads(_s: str) -> list[tuple[int, str, str, int]]:
        return _Existing()

    monkeypatch.setattr(loop.json, "loads", fake_loads)

    loop.run_once()

    with sqlite3.connect(db_path) as check:
        row = check.execute(
            "SELECT part_count, has_parts, segments FROM release WHERE norm_title = 'example'",
        ).fetchone()
    assert row[0] == 0
    assert row[1] == 0
    assert row[2] == "[]"
