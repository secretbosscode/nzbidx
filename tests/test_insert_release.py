from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import insert_release, CATEGORY_MAP  # type: ignore


def test_insert_release_filters_surrogates() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ)"
    )
    inserted = insert_release(
        conn,
        "foo\udc80bar",
        "cat\udc80",
        "en",
        ["tag\udc80"],
        "alt.binaries.example",
        123,
        "2024-02-01T00:00:00+00:00",
    )
    assert inserted == {"foobar"}
    row = conn.execute(
        "SELECT norm_title, category, language, tags, source_group, size_bytes, posted_at FROM release",
    ).fetchone()
    assert row == (
        "foobar",
        "cat",
        "en",
        "tag",
        "alt.binaries.example",
        123,
        "2024-02-01T00:00:00+00:00",
    )


def test_insert_release_defaults() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ)"
    )
    inserted = insert_release(conn, "foo", None, None, None, None, None, None)
    assert inserted == {"foo"}
    row = conn.execute(
        "SELECT norm_title, category, language, tags, source_group, size_bytes, posted_at FROM release",
    ).fetchone()
    assert row == ("foo", CATEGORY_MAP["other"], "und", "", None, None, None)


def test_insert_release_batch() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT UNIQUE, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ)"
    )
    releases = [
        ("foo", None, None, None, None, None, None),
        (
            "bar",
            "cat",
            "en",
            ["tag"],
            "alt.binaries.example",
            456,
            "2024-02-01T00:00:00+00:00",
        ),
        ("foo", None, None, None, None, None, None),
    ]
    inserted = insert_release(conn, releases=releases)
    assert inserted == {"foo", "bar"}
    rows = conn.execute(
        "SELECT norm_title, size_bytes, posted_at FROM release ORDER BY norm_title"
    ).fetchall()
    assert rows == [
        ("bar", 456, "2024-02-01T00:00:00+00:00"),
        ("foo", None, None),
    ]


