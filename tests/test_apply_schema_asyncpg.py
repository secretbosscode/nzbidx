from __future__ import annotations

import asyncio
import uuid

import pytest

from nzbidx_api import db


def test_apply_schema_asyncpg(monkeypatch) -> None:
    psycopg = pytest.importorskip("psycopg")
    asyncpg = pytest.importorskip("asyncpg")
    pytest.importorskip("sqlalchemy")

    dbname = f"test_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - env specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    async def run() -> None:
        monkeypatch.setattr(
            db,
            "DATABASE_URL",
            f"postgresql+asyncpg://root@/{dbname}",
            raising=False,
        )
        await db.dispose_engine()
        await db.init_engine()
        try:
            await db.apply_schema()
        except asyncpg.exceptions.ActiveSQLTransactionError as exc:  # pragma: no cover - regression
            pytest.fail(f"ActiveSQLTransactionError: {exc}")
        finally:
            await db.dispose_engine()

    asyncio.run(run())

    admin = psycopg.connect(dbname="postgres", user="root")
    admin.autocommit = True
    admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
    admin.close()

