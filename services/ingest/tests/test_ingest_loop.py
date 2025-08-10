"""Tests for the header ingest loop."""

from pathlib import Path
import sys

import sqlite3

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_ingest import ingest_loop, cursors  # noqa: E402


def test_run_once_processes_headers(monkeypatch):
    monkeypatch.setenv("NNTP_GROUPS", "alt.test")
    ingest_loop.NNTP_GROUPS = ["alt.test"]

    headers = [
        {"subject": "Great Song [music]"},
        {"subject": "Another Tune [music]"},
    ]

    class DummyClient:
        def connect(self):
            pass

        def xover(self, group: str, start: int, end: int):
            assert group == "alt.test"
            return headers

    monkeypatch.setattr(ingest_loop, "NNTPClient", lambda: DummyClient())

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (id INTEGER PRIMARY KEY AUTOINCREMENT, norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT)"
    )
    monkeypatch.setattr(ingest_loop, "connect_db", lambda: conn)
    monkeypatch.setattr(ingest_loop, "connect_opensearch", lambda: None)

    inserted: list[str] = []
    indexed: list[str] = []

    def fake_insert(db, norm_title, category, language, tags):
        inserted.append(norm_title)
        return True

    def fake_index(client, norm_title, *, category=None, language=None, tags=None):
        indexed.append(norm_title)

    monkeypatch.setattr(ingest_loop, "insert_release", fake_insert)
    monkeypatch.setattr(ingest_loop, "index_release", fake_index)
    monkeypatch.setattr(cursors, "get_cursor", lambda g: 0)
    last: dict[str, int] = {}
    monkeypatch.setattr(cursors, "set_cursor", lambda g, v: last.setdefault(g, v))

    ingest_loop.run_once()

    assert inserted == ["great song", "another tune"]
    assert indexed == ["great song", "another tune"]
    assert last["alt.test"] == 2
