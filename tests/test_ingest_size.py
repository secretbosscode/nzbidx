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


def test_ingested_releases_include_size(monkeypatch) -> None:
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
            return 1

        def xover(self, group: str, start: int, end: int):
            return [{"subject": "Example", ":bytes": "456"}]

    monkeypatch.setattr(loop, "NNTPClient", lambda: DummyClient())
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT)"
    )
    monkeypatch.setattr(loop, "connect_db", lambda: conn)
    monkeypatch.setattr(loop, "connect_opensearch", lambda: object())

    def fake_bulk(_client, docs):
        captured.extend(docs)

    monkeypatch.setattr(loop, "bulk_index_releases", fake_bulk)

    loop.run_once()

    assert captured
    doc_id, body = captured[0]
    assert body.get("size_bytes") == 456
    row = conn.execute(
        "SELECT norm_title, size_bytes FROM release"
    ).fetchone()
    assert row == ("example", 456)

    class DummySearchClient:
        def search(self, **kwargs):
            return {"hits": {"hits": [{"_id": doc_id, "_source": body}]}}

    def dummy_call_with_retry(_breaker, _dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    items = search_mod.search_releases(DummySearchClient(), {"must": []}, limit=1)
    assert items[0]["size"] == "456"
