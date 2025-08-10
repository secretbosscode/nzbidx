from __future__ import annotations

import sqlite3
import time
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from nzbidx_ingest import ingest_loop  # noqa: E402


def test_backpressure_and_metrics(monkeypatch, caplog) -> None:
    monkeypatch.setenv("NNTP_GROUPS", "alt.test")
    ingest_loop.NNTP_GROUPS = ["alt.test"]
    ingest_loop.INGEST_OS_LATENCY_MS = 10

    headers = [{"subject": "Test [music]"}]

    class DummyClient:
        def connect(self):
            pass

        def xover(self, group: str, start: int, end: int):
            return headers

    monkeypatch.setattr(ingest_loop, "NNTPClient", lambda: DummyClient())

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (id INTEGER PRIMARY KEY AUTOINCREMENT, norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT)"
    )
    monkeypatch.setattr(ingest_loop, "connect_db", lambda: conn)
    monkeypatch.setattr(ingest_loop, "connect_opensearch", lambda: object())

    def fake_insert(db, norm_title, category, language, tags):
        return True

    orig_sleep = time.sleep

    def fake_index(client, norm_title, *, category=None, language=None, tags=None):
        orig_sleep(0.04)

    monkeypatch.setattr(ingest_loop, "insert_release", fake_insert)
    monkeypatch.setattr(ingest_loop, "index_release", fake_index)
    monkeypatch.setattr(ingest_loop.cursors, "get_cursor", lambda g: 0)
    monkeypatch.setattr(ingest_loop.cursors, "set_cursor", lambda g, v: None)

    sleeps: list[float] = []

    def record_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(ingest_loop.time, "sleep", record_sleep)

    with caplog.at_level("INFO"):
        ingest_loop.run_once()

    assert sleeps and 0 < sleeps[-1] <= 0.03
    record = next(r for r in caplog.records if r.message == "ingest_batch")
    assert record.processed == 1
    assert record.indexed == 1
    assert record.os_latency_ms > 0
