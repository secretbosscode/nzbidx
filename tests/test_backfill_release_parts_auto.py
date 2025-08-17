from __future__ import annotations

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
            has_parts BOOLEAN
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE release_part (
            release_id INTEGER,
            number INT,
            message_id TEXT,
            source_group TEXT,
            size_bytes INT
        )
        """
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, has_parts) VALUES (1, 'r1', 'g1', 1)"
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, has_parts) VALUES (2, 'r2', 'g1', 1)"
    )
    cur.execute(
        "INSERT INTO release_part (release_id, number, message_id, source_group, size_bytes) VALUES (2, 1, 'm2', 'g1', 10)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))
    monkeypatch.setattr(backfill_mod, "connect_opensearch", lambda: None)
    monkeypatch.setattr(backfill_mod, "bulk_index_releases", lambda *a, **k: None)
    monkeypatch.setattr(backfill_mod, "_fetch_segments", lambda _id: [(1, "m1", 5)])

    processed = backfill_mod.backfill_release_parts(release_ids=[1])
    assert processed == 1

    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM release_part WHERE release_id = 1")
    assert cur2.fetchone()[0] == 1
    cur2.execute("SELECT COUNT(*) FROM release_part WHERE release_id = 2")
    assert cur2.fetchone()[0] == 1
    conn2.close()
