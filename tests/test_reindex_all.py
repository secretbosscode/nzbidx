from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from scripts import reindex_all  # type: ignore


def test_reindex_all(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE release (
            id INTEGER,
            norm_title TEXT,
            category TEXT,
            language TEXT,
            tags TEXT,
            source_group TEXT,
            size_bytes BIGINT,
            posted_at TEXT,
            has_parts BOOLEAN,
            part_count INT
        )
        """
    )
    conn.execute(
        "INSERT INTO release VALUES (1, 'r1', 'c1', 'en', 'tag1,tag2', 'g1', 100, '2024-01-01T00:00:00Z', 1, 2)"
    )
    conn.execute(
        "INSERT INTO release VALUES (2, 'r2', 'c2', 'en', '', 'g2', 200, '2024-01-02T00:00:00Z', 0, 0)"
    )
    conn.commit()

    captured: list[tuple[str, dict[str, object]]] = []

    def fake_bulk(client, docs):
        captured.extend(docs)

    monkeypatch.setattr(reindex_all, "connect_db", lambda: conn)
    monkeypatch.setattr(reindex_all, "connect_opensearch", lambda: object())
    monkeypatch.setattr(reindex_all, "bulk_index_releases", fake_bulk)

    reindex_all.reindex_all(batch_size=1)

    assert captured == [
        (
            "r1",
            {
                "norm_title": "r1",
                "has_parts": True,
                "part_count": 2,
                "category": "c1",
                "language": "en",
                "tags": ["tag1", "tag2"],
                "source_group": "g1",
                "size_bytes": 100,
                "posted_at": "2024-01-01T00:00:00Z",
            },
        )
    ]
