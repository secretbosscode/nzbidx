from __future__ import annotations

import asyncio
import importlib
import logging

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from nzbidx_api import db

m_posted = importlib.import_module("nzbidx_api.migrations.0001_add_posted_at_index")


def test_apply_schema_logs_migration_error(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setattr(db, "get_engine", lambda: engine)

    monkeypatch.setattr(
        db,
        "_read_schema_text",
        lambda: "CREATE TABLE release (id INTEGER PRIMARY KEY, posted_at TIMESTAMP);",
    )
    db.load_schema_statements.cache_clear()

    def migrate_posted(_conn):
        raise RuntimeError("boom")

    monkeypatch.setattr(m_posted, "migrate", migrate_posted)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(db.apply_schema())
    assert "migration_failed" in caplog.text

    asyncio.run(engine.dispose())
