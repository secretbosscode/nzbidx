from __future__ import annotations

import asyncio
import uuid

import pytest

from nzbidx_api import db as api_db
from nzbidx_api.search import search_releases_async
from nzbidx_ingest.main import connect_db


def test_search_vector_autocreate(monkeypatch) -> None:
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("asyncpg")
    pytest.importorskip("sqlalchemy")

    dbname = f"test_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")

    url_psycopg = f"postgresql+psycopg://root@/{dbname}"
    url_asyncpg = f"postgresql+asyncpg://root@/{dbname}"

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - env specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    # Create initial schema without search_vector column
    monkeypatch.setenv("DATABASE_URL", url_psycopg)
    conn = connect_db()
    conn.close()

    # Ensure column is absent prior to API startup
    check = psycopg.connect(dbname=dbname, user="root")
    cur = check.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='release' AND column_name='search_vector'"
    )
    assert cur.fetchone() is None
    cur.close()
    check.close()

    async def run() -> None:
        monkeypatch.setattr(api_db, "DATABASE_URL", url_asyncpg, raising=False)
        await api_db.dispose_engine()
        await api_db.init_engine()
        await api_db.apply_schema()

        # Verify column and index now exist
        check2 = psycopg.connect(dbname=dbname, user="root")
        cur2 = check2.cursor()
        cur2.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name='release' AND column_name='search_vector'"
        )
        assert cur2.fetchone() is not None
        cur2.execute(
            "SELECT 1 FROM pg_indexes "
            "WHERE tablename='release' AND indexname='release_search_idx'"
        )
        assert cur2.fetchone() is not None
        cur2.close()
        check2.close()

        # Search query should succeed without ProgrammingError
        results = await search_releases_async("foo", limit=1)
        assert results == []

        await api_db.dispose_engine()

    asyncio.run(run())

    admin = psycopg.connect(dbname="postgres", user="root")
    admin.autocommit = True
    admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
    admin.close()
