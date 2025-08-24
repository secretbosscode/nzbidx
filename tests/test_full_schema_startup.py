from __future__ import annotations

import asyncio
import inspect

import pytest

import nzbidx_api.main as main
from nzbidx_api import db


@pytest.mark.parametrize("with_ingest", [True, False])
def test_full_schema_startup(monkeypatch, with_ingest) -> None:
    executed: list[str] = []

    class DummyConn:
        async def __aenter__(self):  # pragma: no cover - simple
            return self

        async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - simple
            return None

        async def execute(self, stmt, params=None):  # pragma: no cover - simple
            executed.append(str(stmt))

        async def commit(self):  # pragma: no cover - simple
            return None

        async def rollback(self):  # pragma: no cover - simple
            return None

        async def scalar(self, stmt, params=None):  # pragma: no cover - simple
            return 1

    class DummyRaw:
        def close(self):  # pragma: no cover - simple
            return None

    class DummySync:
        def raw_connection(self):  # pragma: no cover - simple
            return DummyRaw()

    class DummyEngine:
        def __init__(self):  # pragma: no cover - simple
            self.sync_engine = DummySync()

        def connect(self):  # pragma: no cover - simple
            return DummyConn()

    dummy_engine = DummyEngine()

    monkeypatch.setattr(db, "create_async_engine", lambda *a, **k: dummy_engine)
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "migrate_release_adult_partitions", lambda raw: None)
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_engine_loop", None)

    if with_ingest:
        called = {"ingest": False}

        def fake_start_ingest() -> None:  # pragma: no cover - simple
            called["ingest"] = True

        monkeypatch.setattr(main, "start_ingest", fake_start_ingest)
    else:
        monkeypatch.setattr(main, "start_ingest", lambda: None)

    monkeypatch.setattr(main, "start_auto_backfill", lambda: None)
    monkeypatch.setattr(main, "start_metrics", lambda: None)
    monkeypatch.setattr(main, "_set_stop", lambda cb: None)

    startup_funcs = [
        main.init_engine,
        main.apply_schema,
        main.start_ingest,
        main.start_auto_backfill,
        lambda: main._set_stop(main.start_metrics()),
    ]
    monkeypatch.setattr(main.app, "on_startup", startup_funcs)

    async def _run_startup() -> None:
        for fn in main.app.on_startup:
            if inspect.iscoroutinefunction(fn):
                await fn()
            else:
                fn()

    asyncio.run(_run_startup())

    if with_ingest:
        assert called["ingest"]

    def _has(pattern: str) -> bool:
        return any(pattern in stmt for stmt in executed)

    columns = [
        "ADD COLUMN IF NOT EXISTS norm_title",
        "ADD COLUMN IF NOT EXISTS category",
        "ADD COLUMN IF NOT EXISTS category_id",
        "ADD COLUMN IF NOT EXISTS language",
        "ADD COLUMN IF NOT EXISTS tags",
        "ADD COLUMN IF NOT EXISTS source_group",
        "ADD COLUMN IF NOT EXISTS size_bytes",
        "ADD COLUMN IF NOT EXISTS posted_at",
        "ADD COLUMN IF NOT EXISTS segments",
        "ADD COLUMN IF NOT EXISTS has_parts",
        "ADD COLUMN IF NOT EXISTS part_count",
    ]
    indexes = [
        "release_category_idx",
        "release_category_id_idx",
        "release_language_idx",
        "release_tags_idx",
        "release_norm_title_idx",
        "release_source_group_idx",
        "release_size_bytes_idx",
        "release_norm_title_category_id_posted_at_key",
    ]
    for pattern in columns + indexes:
        assert _has(pattern), pattern
