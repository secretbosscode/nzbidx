from __future__ import annotations

import sqlite3
import time


def test_auto_backfill_runs_on_startup(tmp_path, monkeypatch) -> None:
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
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, segments, part_count) VALUES (1, 'r1', 'g1', 0, 1, NULL, 0)"
    )
    conn.commit()
    conn.close()

    from nzbidx_api import main

    def dummy_backfill(progress_cb=None, release_ids=None, auto=False):
        assert auto
        conn2 = sqlite3.connect(dbfile)
        conn2.execute("UPDATE release SET segments='[]' WHERE id=1")
        conn2.commit()
        conn2.close()
        if progress_cb:
            progress_cb(1)
        return 1

    monkeypatch.setattr(main, "backfill_release_parts", dummy_backfill)

    assert main.start_auto_backfill in main.app.on_startup

    main.start_auto_backfill()

    for _ in range(20):
        conn3 = sqlite3.connect(dbfile)
        cur3 = conn3.cursor()
        cur3.execute("SELECT segments FROM release WHERE id = 1")
        seg = cur3.fetchone()[0]
        conn3.close()
        if seg is not None:
            break
        time.sleep(0.05)
    else:
        assert False, "backfill did not run"
    assert seg == "[]"
