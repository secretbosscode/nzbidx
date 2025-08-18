from __future__ import annotations

import json
import sqlite3
from nzbidx_api import backfill_release_parts as backfill_mod


def test_backfill_specific_ids(tmp_path, monkeypatch) -> None:
    """Only releases listed via ``release_ids`` should be processed."""

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
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, segments, part_count) VALUES (2, 'r2', 'g1', 10, 1, ?, 1)",
        ('[{"number":1,"message_id":"m2","group":"g1","size":10}]',),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))
    monkeypatch.setattr(
        backfill_mod, "_fetch_segments", lambda _id, _group: [(1, "m1", 5)]
    )

    processed = backfill_mod.backfill_release_parts(release_ids=[1])
    assert processed == 1

    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT segments FROM release WHERE id = 1")
    seg1 = json.loads(cur2.fetchone()[0])
    assert seg1[0]["message_id"] == "m1"
    cur2.execute("SELECT segments FROM release WHERE id = 2")
    seg2 = json.loads(cur2.fetchone()[0])
    assert seg2[0]["message_id"] == "m2"
    conn2.close()
