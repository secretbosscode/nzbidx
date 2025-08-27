import sqlite3

import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore


def test_ingest_strips_nul_bytes(monkeypatch, tmp_path) -> None:
    """Ensure NUL bytes in ``norm_title`` are stripped during ingest."""
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
            return [
                {
                    ":bytes": "100",
                    "subject": "Example\x00(1/1)",
                    "message-id": "<m1>",
                }
            ]

        def body_size(self, _mid: str) -> int:
            return 100

    monkeypatch.setattr(loop, "NNTPClient", lambda _settings: DummyClient())

    db_path = tmp_path / "db.sqlite"

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, has_parts INT NOT NULL DEFAULT 0, part_count INT NOT NULL DEFAULT 0, segments TEXT, UNIQUE (norm_title, category_id, posted_at))"
        )
        conn.commit()
        return conn

    monkeypatch.setattr(loop, "connect_db", _connect)

    from nzbidx_ingest.main import insert_release  # type: ignore

    monkeypatch.setattr(loop, "insert_release", insert_release)

    # Should not raise despite the NUL byte
    loop.run_once()

    with sqlite3.connect(db_path) as check:
        row = check.execute(
            "SELECT norm_title FROM release WHERE source_group = 'alt.test'"
        ).fetchone()
    assert row is not None
    assert row[0] == "example"
