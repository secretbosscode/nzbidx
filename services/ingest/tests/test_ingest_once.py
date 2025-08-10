"""Tests for ingest loop deduplication."""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_ingest.ingest_loop as ingest_loop  # noqa: E402
import nzbidx_ingest.cursors as cursors  # noqa: E402


def test_ingest_dedup(monkeypatch):
    monkeypatch.setenv("NNTP_GROUPS", "alt.test")
    ingest_loop.NNTP_GROUPS = ["alt.test"]

    headers = [
        {"subject": "Foo", "date": "Thu, 1 Jan 2020 00:00:00 +0000"},
        {"subject": "Foo", "date": "Thu, 1 Jan 2020 00:00:00 +0000"},
        {"subject": "Bar", "date": "Thu, 1 Jan 2020 00:00:00 +0000"},
    ]

    class Client:
        def connect(self):
            pass

        def xover(self, group, start, end):
            return headers

    monkeypatch.setattr(ingest_loop, "NNTPClient", lambda: Client())

    seen: set[str] = set()
    inserted: list[str] = []

    def fake_insert(db, norm_title, category, language, tags):
        if norm_title in seen:
            return False
        seen.add(norm_title)
        inserted.append(norm_title)
        return True

    indexed: list[str] = []

    def fake_index(client, norm_title, *, category=None, language=None, tags=None):
        indexed.append(norm_title)

    monkeypatch.setattr(ingest_loop, "insert_release", fake_insert)
    monkeypatch.setattr(ingest_loop, "index_release", fake_index)
    monkeypatch.setattr(ingest_loop, "connect_db", lambda: object())
    monkeypatch.setattr(ingest_loop, "connect_opensearch", lambda: object())
    monkeypatch.setattr(cursors, "get_cursor", lambda g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda g, v: None)

    ingest_loop.run_once()

    assert inserted == ["foo:2020-01-01", "bar:2020-01-01"]
    assert indexed == ["foo:2020-01-01", "bar:2020-01-01"]
