from __future__ import annotations

import json
import sqlite3

from nzbidx_api import backfill_release_parts as backfill_mod


def test_auto_backfill_batches_progress_and_deletes(tmp_path, monkeypatch) -> None:
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
        """,
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, segments, part_count) VALUES (1, 'r1', 'g1', 0, 1, NULL, 0)"
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, segments, part_count) VALUES (2, 'r2', 'g1', 0, 1, NULL, 0)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))
    monkeypatch.setattr(backfill_mod.config, "NNTP_GROUPS", ["g1"], raising=False)
    monkeypatch.setattr(backfill_mod, "BATCH_SIZE", 1, raising=False)

    def _fetch_segments(norm_title: str, group: str):
        if norm_title == "r1":
            return [(1, "m1", 5)]
        return []

    monkeypatch.setattr(backfill_mod, "_fetch_segments", _fetch_segments)

    progress: list[int] = []
    processed = backfill_mod.backfill_release_parts(
        auto=True, progress_cb=lambda p: progress.append(p)
    )
    assert processed == 1
    assert progress == [1]

    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT segments FROM release WHERE id = 1")
    seg1_json = cur2.fetchone()[0]
    assert json.loads(seg1_json)[0]["message_id"] == "m1"
    cur2.execute("SELECT COUNT(*) FROM release WHERE id = 2")
    assert cur2.fetchone()[0] == 0
    conn2.close()
