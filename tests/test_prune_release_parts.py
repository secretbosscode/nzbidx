from __future__ import annotations

import sys
from pathlib import Path
import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.main as main  # type: ignore


def test_pruned_releases_clear_parts(monkeypatch, tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "db.sqlite")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS release (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            norm_title TEXT UNIQUE,
            category TEXT,
            language TEXT,
            tags TEXT,
            source_group TEXT,
            size_bytes BIGINT,
            has_parts INT NOT NULL DEFAULT 0,
            part_count INT NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS release_part (
            release_id INT,
            segment_number INT,
            message_id TEXT,
            group_name TEXT,
            size_bytes BIGINT,
            PRIMARY KEY (release_id, segment_number)
        )
        """
    )
    conn.execute(
        "INSERT INTO release (norm_title, has_parts, part_count) VALUES ('old', 1, 2)"
    )
    conn.execute(
        "INSERT INTO release (norm_title, has_parts, part_count) VALUES ('mid', 1, 1)"
    )
    conn.execute(
        "INSERT INTO release (norm_title, has_parts, part_count) VALUES ('new', 1, 1)"
    )
    conn.execute(
        "INSERT INTO release_part (release_id, segment_number, message_id, group_name, size_bytes) VALUES (1, 1, '<a>', 'alt.test', 10)"
    )
    conn.execute(
        "INSERT INTO release_part (release_id, segment_number, message_id, group_name, size_bytes) VALUES (1, 2, '<b>', 'alt.test', 20)"
    )
    conn.execute(
        "INSERT INTO release_part (release_id, segment_number, message_id, group_name, size_bytes) VALUES (2, 1, '<c>', 'alt.test', 30)"
    )
    conn.commit()

    captured: list[tuple[str, object | None]] = []

    def fake_bulk(_client, docs) -> None:
        captured.extend(docs)

    monkeypatch.setattr(main, "bulk_index_releases", fake_bulk)

    main.prune_release_parts(conn, max_releases=1, client=object())

    assert (
        conn.execute(
            "SELECT COUNT(*) FROM release_part WHERE release_id = 1"
        ).fetchone()[0]
        == 0
    )
    assert conn.execute(
        "SELECT has_parts, part_count FROM release WHERE id = 1"
    ).fetchone() == (0, 0)
    assert captured == [("old", None)]
    assert conn.execute(
        "SELECT has_parts, part_count FROM release WHERE id = 2"
    ).fetchone() == (1, 1)
