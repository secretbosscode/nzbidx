from __future__ import annotations

import json
import sqlite3

# ruff: noqa: E402
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

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

    processed = backfill_mod.backfill_release_parts(release_ids=[1])
    assert processed == 1

    conn2 = sqlite3.connect(dbfile)
    cur2 = conn2.cursor()
    cur2.execute("SELECT segments, part_count, size_bytes FROM release WHERE id = 1")
    seg_json, part_count, size_bytes = cur2.fetchone()
    segments = json.loads(seg_json)
    assert segments[0]["message_id"] == "m1"
    assert segments[1]["message_id"] == "m2"
    assert part_count == 2
    assert size_bytes == 25
    conn2.close()
