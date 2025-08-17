from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sys
from pathlib import Path
import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore


def test_duplicate_headers_not_counted(monkeypatch, tmp_path) -> None:
    captured: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 3

        def xover(self, group: str, start: int, end: int):
            return [
                {"subject": "Example (1/2)", ":bytes": "100", "message-id": "<a>"},
                {"subject": "Example (1/2)", ":bytes": "100", "message-id": "<a>"},
                {"subject": "Example (2/2)", ":bytes": "200", "message-id": "<b>"},
            ]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, segments TEXT, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0)"
        )
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: object())

    def fake_bulk(_client, docs):
        captured.extend(docs)

    monkeypatch.setattr(loop, "bulk_index_releases", fake_bulk)

    loop.run_once()

    assert captured
    _doc_id, body = captured[0]
    assert body.get("size_bytes") == 300
    assert body.get("part_count") == 2

    with sqlite3.connect(db_path) as check:
        row = check.execute("SELECT size_bytes, part_count FROM release").fetchone()
    assert row == (300, 2)
