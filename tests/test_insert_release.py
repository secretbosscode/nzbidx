from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import insert_release  # type: ignore


def test_insert_release_filters_surrogates() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT)"
    )
    inserted = insert_release(
        conn,
        "foo\udc80bar",
        "cat\udc80",
        "en",
        ["tag\udc80"],
    )
    assert inserted
    row = conn.execute(
        "SELECT norm_title, category, language, tags FROM release"
    ).fetchone()
    assert row == ("foobar", "cat", "en", "tag")
