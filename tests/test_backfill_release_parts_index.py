from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# ruff: noqa: E402 - path manipulation before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import backfill_release_parts as backfill_mod  # type: ignore


def test_size_bytes_updated(tmp_path, monkeypatch) -> None:
    dbfile = tmp_path / "test.db"
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE release (
            id INTEGER PRIMARY KEY,
            norm_title TEXT UNIQUE,
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
    monkeypatch.setattr(backfill_mod, "bulk_index_releases", lambda *a, **k: None)
    monkeypatch.setattr(backfill_mod, "_fetch_segments", lambda _id, _group: [(1, "m1", 100)])

    processed = backfill_mod.backfill_release_parts()
    assert processed == 1
    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT size_bytes FROM release WHERE id = 1")
    assert cur2.fetchone()[0] == 100
    conn2.close()
