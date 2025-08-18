from __future__ import annotations

import sqlite3

from nzbidx_ingest.main import insert_release  # type: ignore
from scripts.normalize_releases import normalize_releases  # type: ignore


def test_normalize_releases_merges_parts() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE (norm_title, category_id))"
    )
    insert_release(
        conn,
        "foo.part01.rar:2024-01-01",
        None,
        None,
        None,
        None,
        100,
        None,
    )
    insert_release(
        conn,
        "foo.part02.rar:2024-01-01",
        None,
        None,
        None,
        None,
        200,
        None,
    )

    normalize_releases(conn)

    rows = conn.execute("SELECT norm_title, size_bytes FROM release").fetchall()
    assert rows == [("foo:2024-01-01", 300)]
