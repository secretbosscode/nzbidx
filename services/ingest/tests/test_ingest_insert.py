from __future__ import annotations

import sqlite3
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_common.os import OS_RELEASES_ALIAS
from nzbidx_ingest.main import main


def test_ingest_inserts_and_indexes(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (id INTEGER PRIMARY KEY AUTOINCREMENT, norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT)"
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

    rows = conn.execute(
        "SELECT norm_title, category, language, tags FROM release ORDER BY norm_title"
    ).fetchall()
    assert rows == [
        ("another release", "7000", "en", "books"),
        ("test release one", "3000", "en", "music"),
    ]

    assert [c["body"]["category"] for c in dummy_os.calls] == ["3000", "7000"]
    assert [c["body"]["tags"] for c in dummy_os.calls] == [["music"], ["books"]]
    assert [c["index"] for c in dummy_os.calls] == [
        OS_RELEASES_ALIAS,
        OS_RELEASES_ALIAS,
    ]
