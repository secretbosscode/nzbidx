from __future__ import annotations

import sqlite3

import logging
import types
from nzbidx_ingest.main import insert_release, CATEGORY_MAP  # type: ignore


def test_insert_release_filters_surrogates() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE(norm_title, category_id, posted_at))",
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
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE(norm_title, category_id, posted_at))",
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
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE(norm_title, category_id, posted_at))",
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
        "SELECT norm_title, size_bytes, posted_at FROM release ORDER BY norm_title",
    ).fetchall()
    assert rows == [
        ("bar", 456, "2024-02-01T00:00:00+00:00"),
        ("foo", None, None),
    ]


def test_insert_release_same_title_different_category() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE(norm_title, category_id, posted_at))",
    )
    insert_release(conn, "foo", CATEGORY_MAP["movies"], None, None, None, None, None)
    insert_release(conn, "foo", CATEGORY_MAP["audio"], None, None, None, None, None)
    rows = conn.execute(
        "SELECT norm_title, category_id FROM release ORDER BY category_id",
    ).fetchall()
    assert rows == [
        ("foo", int(CATEGORY_MAP["movies"])),
        ("foo", int(CATEGORY_MAP["audio"])),
    ]


def test_insert_release_updates_matching_category() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT, category TEXT, category_id INT, language TEXT, tags TEXT, source_group TEXT, size_bytes BIGINT, posted_at TIMESTAMPTZ, UNIQUE(norm_title, category_id, posted_at))",
    )
    insert_release(conn, "foo", CATEGORY_MAP["movies"], None, None, None, None, None)
    insert_release(conn, "foo", CATEGORY_MAP["audio"], None, None, None, None, None)
    insert_release(
        conn,
        "foo",
        CATEGORY_MAP["audio"],
        None,
        None,
        None,
        None,
        "2024-02-01T00:00:00+00:00",
    )
    rows = conn.execute(
        "SELECT category_id, posted_at FROM release WHERE norm_title = 'foo' ORDER BY category_id",
    ).fetchall()
    assert rows == [
        (int(CATEGORY_MAP["movies"]), None),
        (int(CATEGORY_MAP["audio"]), "2024-02-01T00:00:00+00:00"),
    ]


def test_insert_release_skips_data_error(monkeypatch, caplog) -> None:
    from nzbidx_ingest import main

    class FakeDataError(Exception):
        pass

    monkeypatch.setattr(
        main,
        "psycopg",
        types.SimpleNamespace(DataError=FakeDataError),
    )

    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn
            self._results = []

        def execute(self, sql, params=None):
            if sql.startswith("SELECT"):
                self._results = []
            else:
                title = params[0]
                if title == "bad":
                    raise FakeDataError("bad")
                self.conn.rows.append(title)
            return self

        def executemany(self, sql, rows):
            for row in rows:
                title = row[0]
                if title == "bad":
                    raise FakeDataError("bad")
            for row in rows:
                title = row[0]
                self.conn.rows.append(title)
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    class DummyConn:
        def __init__(self):
            self.rows: list[str] = []

        def cursor(self):
            return DummyCursor(self)

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    DummyConn.__module__ = "psycopg"

    conn = DummyConn()
    releases = [
        ("good", "cat", "en", [], "group", 100, None),
        ("bad", "cat", "en", [], "group", 100, None),
    ]
    with caplog.at_level(logging.WARNING):
        inserted = main.insert_release(conn, releases=releases)
    assert inserted == {"good"}
    assert conn.rows == ["good"]
    assert any(
        record.message == "insert_release_data_error" and record.norm_title == "bad"
        for record in caplog.records
    )
