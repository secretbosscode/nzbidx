from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sys
from pathlib import Path
import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_ingest.main as main  # type: ignore


def test_prune_orphaned_releases(monkeypatch) -> None:
    """Only OpenSearch documents without DB records should be removed."""

    deleted: list[dict[str, object]] = []

    class DummyClient:
        def search(self, *, index, scroll, size, body):  # type: ignore[override]
            return {
                "_scroll_id": "1",
                "hits": {"hits": [{"_id": "keep"}, {"_id": "drop"}]},
            }

        def scroll(self, scroll_id, scroll):  # type: ignore[override]
            return {"_scroll_id": "1", "hits": {"hits": []}}

        def clear_scroll(self, scroll_id):  # type: ignore[override]
            pass

        def delete_by_query(self, *, index, body):  # type: ignore[override]
            deleted.append(body)

    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE release (norm_title TEXT)")
    db.execute("INSERT INTO release (norm_title) VALUES ('keep')")
    db.commit()
    monkeypatch.setattr(main, "connect_db", lambda: db)

    count = main.prune_orphaned_releases(DummyClient())

    assert count == 1
    assert deleted == [{"query": {"ids": {"values": ["drop"]}}}]
