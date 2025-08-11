"""Tests for ingest progress logging."""

from __future__ import annotations

import logging

from nzbidx_ingest import ingest_loop, cursors


def test_ingest_progress_logging(monkeypatch, caplog) -> None:
    """Run a single ingest batch and verify progress metrics are logged."""
    caplog.set_level(logging.INFO)

    # Use a single test group with a tiny batch size
    monkeypatch.setattr(ingest_loop, "NNTP_GROUPS", ["test.group"])
    monkeypatch.setattr(ingest_loop, "INGEST_BATCH", 2)

    # Provide a deterministic NNTP client
    headers = [
        {"subject": "Release 1", "date": "Mon, 01 Jan 2024 00:00:00 +0000"},
        {"subject": "Release 2", "date": "Mon, 01 Jan 2024 00:00:00 +0000"},
    ]

    class DummyClient:
        def connect(self) -> None:  # pragma: no cover - trivial
            pass

        def xover(self, group: str, start: int, end: int):  # pragma: no cover - trivial
            return headers

        def high_water_mark(self, group: str) -> int:  # pragma: no cover - trivial
            return 10

    monkeypatch.setattr(ingest_loop, "NNTPClient", lambda: DummyClient())

    # Stub out database and search interactions
    monkeypatch.setattr(ingest_loop, "connect_db", lambda: None)
    monkeypatch.setattr(ingest_loop, "connect_opensearch", lambda: None)
    monkeypatch.setattr(ingest_loop, "insert_release", lambda *a, **k: True)
    monkeypatch.setattr(ingest_loop, "index_release", lambda *a, **k: None)

    # In-memory cursor storage
    state: dict[str, int] = {}
    monkeypatch.setattr(cursors, "get_cursor", lambda group: state.get(group))
    monkeypatch.setattr(cursors, "set_cursor", lambda group, last: state.__setitem__(group, last))

    # Deterministic timing so ETA calculation is stable
    t = [0.0]

    def fake_monotonic() -> float:  # pragma: no cover - deterministic
        t[0] += 0.1
        return t[0]

    monkeypatch.setattr(ingest_loop.time, "monotonic", fake_monotonic)

    ingest_loop.run_once()

    record = next(r for r in caplog.records if r.msg == "ingest_batch")
    assert record.high_water == 10
    assert record.remaining == 8
    assert record.eta_s > 0

