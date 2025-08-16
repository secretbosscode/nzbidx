from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sys
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore


def test_ingest_batch_log(monkeypatch, caplog) -> None:
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    class DummyClient:
        def connect(self) -> None:
            pass

        def high_water_mark(self, group: str) -> int:
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{":bytes": "100", "subject": "Example"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: None)
    monkeypatch.setattr(loop, "insert_release", lambda _db, releases: {r[0] for r in releases})
    monkeypatch.setattr(loop, "bulk_index_releases", lambda *_args, **_kwargs: None)

    with caplog.at_level(logging.INFO):
        loop.run_once()

    record = next(r for r in caplog.records if r.message.startswith("Processed"))
    assert "Processed 1 items (inserted 1, deduplicated 0)." in record.message
    assert not hasattr(record, "avg_batch_ms")
    assert not hasattr(record, "os_latency_ms")
    assert not hasattr(record, "avg_db_ms")
    assert not hasattr(record, "avg_os_ms")
    assert not hasattr(record, "pct_complete")
    assert not hasattr(record, "eta_s")
    assert not hasattr(record, "deduped")
