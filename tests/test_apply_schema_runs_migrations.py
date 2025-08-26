from __future__ import annotations

import asyncio
import importlib
import sqlite3

from sqlalchemy.ext.asyncio import create_async_engine

from nzbidx_api import db

m_posted = importlib.import_module("nzbidx_api.migrations.0001_add_posted_at_index")


def test_apply_schema_runs_migrations(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setattr(db, "get_engine", lambda: engine)

    class DummyResource:
        def joinpath(self, name: str):
            return self

        def read_text(self, encoding: str = "utf-8") -> str:
            return "CREATE TABLE release (id INTEGER PRIMARY KEY, posted_at TIMESTAMP);"

    monkeypatch.setattr(db.resources, "files", lambda pkg: DummyResource())

    def migrate_posted(conn):
        cur = conn.cursor()
        cur.execute(
            "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release(posted_at)"
        )
        conn.commit()

    monkeypatch.setattr(m_posted, "migrate", migrate_posted)

    asyncio.run(db.apply_schema())

    conn = sqlite3.connect(db_path)
    try:
        idxs = [
            row[1] for row in conn.execute("PRAGMA index_list('release')").fetchall()
        ]
        assert "release_posted_at_idx" in idxs
    finally:
        conn.close()

    asyncio.run(engine.dispose())
