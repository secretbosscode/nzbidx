import logging
import uuid

import pytest
from nzbidx_ingest.main import connect_db

psycopg = pytest.importorskip("psycopg")


def test_connect_db_requires_pg_trgm(monkeypatch, caplog):
    """connect_db should raise if pg_trgm is unavailable."""
    dbname = f"test_{uuid.uuid4().hex}"
    username = f"user_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")
    monkeypatch.setenv("DATABASE_URL", f"postgresql+psycopg://{username}:pw@/{dbname}")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'DROP ROLE IF EXISTS "{username}"')
        admin.execute(f"CREATE ROLE \"{username}\" LOGIN PASSWORD 'pw'")
        admin.execute(f'CREATE DATABASE "{dbname}" OWNER "{username}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    caplog.set_level(logging.WARNING, logger="nzbidx_ingest.main")
    try:
        connect_db()
    except RuntimeError as exc:
        assert "pg_trgm extension is required" in str(exc)
        assert any(rec.msg == "extension_unavailable" for rec in caplog.records)
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    else:  # pragma: no cover - unexpected success
        pytest.fail("connect_db did not raise RuntimeError")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'DROP ROLE IF EXISTS "{username}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
