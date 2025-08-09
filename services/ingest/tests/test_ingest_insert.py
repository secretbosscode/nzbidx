from __future__ import annotations

import sqlite3

from nzbidx_ingest.main import main


def test_ingest_inserts_and_indexes(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (id INTEGER PRIMARY KEY AUTOINCREMENT, norm_title TEXT UNIQUE)"
    )
    monkeypatch.setattr("nzbidx_ingest.main.connect_db", lambda: conn)

    class DummyOS:
        def __init__(self):
            self.calls = []

        def index(self, **kwargs):
            self.calls.append(kwargs)

    dummy_os = DummyOS()
    monkeypatch.setattr("nzbidx_ingest.main.connect_opensearch", lambda: dummy_os)

    monkeypatch.delenv("NNTP_HOST_1", raising=False)

    main()

    rows = conn.execute("SELECT norm_title FROM release ORDER BY norm_title").fetchall()
    assert [r[0] for r in rows] == ["another release", "test release one"]

    assert [c["body"]["norm_title"] for c in dummy_os.calls] == [
        "test release one",
        "another release",
    ]
