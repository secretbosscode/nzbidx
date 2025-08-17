from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import nullcontext
from pathlib import Path

# ruff: noqa: E402 - path manipulation before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import backfill_release_parts as backfill_mod  # type: ignore
from nzbidx_api import search as search_mod  # type: ignore


def test_backfilled_release_indexed(tmp_path, monkeypatch) -> None:
    dbfile = tmp_path / "test.db"
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE release (
            id INTEGER PRIMARY KEY,
            norm_title TEXT UNIQUE,
            source_group TEXT,
            size_bytes BIGINT,
            has_parts BOOLEAN,
            segments TEXT,
            part_count INT
        )
        """
    )
    cur.execute(
        "INSERT INTO release (id, norm_title, source_group, size_bytes, has_parts, part_count) VALUES (1, 'r1', 'g1', 0, 1, 0)"
    )
    conn.commit()
    conn.close()

    class DummyClient:
        def __init__(self) -> None:
            self.docs: dict[str, dict[str, object]] = {}

        def bulk(self, *, body: str, refresh: bool) -> None:  # type: ignore[override]
            lines = body.strip().splitlines()
            for i in range(0, len(lines), 2):
                action = json.loads(lines[i])
                if "index" in action:
                    doc_id = action["index"]["_id"]
                    self.docs[doc_id] = json.loads(lines[i + 1])
                elif "delete" in action:
                    doc_id = action["delete"]["_id"]
                    self.docs.pop(doc_id, None)

        def search(self, **kwargs):  # type: ignore[override]
            hits = [
                {"_id": doc_id, "_source": body}
                for doc_id, body in self.docs.items()
            ]
            return {"hits": {"hits": hits}}

    client = DummyClient()
    monkeypatch.setattr(backfill_mod, "connect_db", lambda: sqlite3.connect(dbfile))
    monkeypatch.setattr(backfill_mod, "connect_opensearch", lambda: client)
    monkeypatch.setattr(backfill_mod, "_fetch_segments", lambda _id: [(1, "m1", 100)])

    processed = backfill_mod.backfill_release_parts()
    assert processed == 1
    assert client.docs["r1"]["part_count"] == 1
    assert client.docs["r1"]["size_bytes"] == 100

    def dummy_call_with_retry(_b, _d, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    items = search_mod.search_releases(client, {"must": []}, limit=1)
    assert items and items[0]["size"] == "100"
