from __future__ import annotations

from nzbidx_api.json_utils import orjson
import sqlite3

import pytest

from nzbidx_api import backfill_release_parts as backfill_mod


class DummyClient:
    def high_water_mark(self, group: str) -> int:
        assert group == "alt.test"
        return 2

    def xover(self, group: str, start: int, end: int):
        return [
            {"subject": "My Release (1/2)", "message-id": "<m1>", "bytes": 10},
            {"subject": "My Release (2/2)", "message-id": "<m2>", "bytes": 15},
        ]

    def body_size(self, message_id: str) -> int:  # pragma: no cover - not used
        return 0


def test_backfill_populates_segments(tmp_path, monkeypatch) -> None:
    dbfile = tmp_path / "test.db"
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE release (
            id INTEGER PRIMARY KEY,
            norm_title TEXT,
            source_group TEXT,
            size_bytes BIGINT,
            has_parts BOOLEAN,
            segments TEXT,
            part_count INT
        )
        """
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, part_count) VALUES (1, 'my release', 'alt.test', 0, 1, 0)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))
    monkeypatch.setattr(backfill_mod, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(backfill_mod.config, "NNTP_GROUPS", ["alt.test"], raising=False)

    processed = backfill_mod.backfill_release_parts(release_ids=[1])
    assert processed == 1

    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT segments, part_count, size_bytes FROM release WHERE id = 1")
    seg_json, part_count, size_bytes = cur2.fetchone()
    segments = orjson.loads(seg_json)
    assert segments[0]["message_id"] == "m1"
    assert segments[1]["message_id"] == "m2"
    assert part_count == 2
    assert size_bytes == 25
    conn2.close()


def test_fetch_segments_connection_error(monkeypatch) -> None:
    class BoomClient:
        def high_water_mark(self, group: str) -> int:
            raise RuntimeError("boom")

    monkeypatch.setattr(backfill_mod, "NNTPClient", lambda: BoomClient())
    with pytest.raises(ConnectionError, match="boom"):
        backfill_mod._fetch_segments("rel", "alt.test")


def test_backfill_propagates_connection_error(tmp_path, monkeypatch) -> None:
    dbfile = tmp_path / "test.db"
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE release (
            id INTEGER PRIMARY KEY,
            norm_title TEXT,
            source_group TEXT,
            size_bytes BIGINT,
            has_parts BOOLEAN,
            segments TEXT,
            part_count INT
        )
        """
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, part_count) VALUES (1, 'r1', 'g1', 0, 1, 0)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))

    def _fail(_id: str, _group: str):
        raise ConnectionError("nntp fail")

    monkeypatch.setattr(backfill_mod, "_fetch_segments", _fail)
    monkeypatch.setattr(backfill_mod.config, "NNTP_GROUPS", ["g1"], raising=False)

    with pytest.raises(ConnectionError, match="nntp fail"):
        backfill_mod.backfill_release_parts(release_ids=[1])
