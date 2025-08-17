from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sys
from pathlib import Path
from contextlib import nullcontext
import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore
import nzbidx_api.search as search_mod  # type: ignore
from nzbidx_ingest.parsers import normalize_subject  # type: ignore


def test_part_rar_segments_collapsed(monkeypatch, tmp_path) -> None:
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
            return 2

        def xover(self, group: str, start: int, end: int):
            return [
                {"subject": "Release.part01.rar", ":bytes": "100"},
                {"subject": "Release.part02.rar", ":bytes": "200"},
            ]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0)"
        )
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: object())

    def fake_bulk(_client, docs):
        captured.extend(docs)

    monkeypatch.setattr(loop, "bulk_index_releases", fake_bulk)

    loop.run_once()

    assert len(captured) == 1
    doc_id, body = captured[0]
    assert body.get("size_bytes") == 300
    with sqlite3.connect(db_path) as check:
        rows = check.execute("SELECT norm_title, size_bytes FROM release").fetchall()
    assert rows == [("release", 300)]



def test_normalize_subject_strips_parts() -> None:
    assert normalize_subject("Example.part01.rar") == "Example"
    assert normalize_subject("Example.part1 par2") == "Example"
    assert normalize_subject("Example part02 zip") == "Example"
