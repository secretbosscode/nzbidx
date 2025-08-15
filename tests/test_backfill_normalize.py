from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import insert_release  # type: ignore
from scripts.normalize_releases import normalize_releases  # type: ignore


def test_normalize_releases_merges_parts() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT)"
    )
    insert_release(
        conn,
        "foo.part01.rar:2024-01-01",
        None,
        None,
        None,
        None,
        100,
    )
    insert_release(
        conn,
        "foo.part02.rar:2024-01-01",
        None,
        None,
        None,
        None,
        200,
    )

    normalize_releases(conn, os_client=None)

    rows = conn.execute("SELECT norm_title, size_bytes FROM release").fetchall()
    assert rows == [("foo:2024-01-01", 300)]
