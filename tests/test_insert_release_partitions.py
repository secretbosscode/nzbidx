import uuid

import pytest
from nzbidx_ingest.main import connect_db, insert_release, CATEGORY_MAP

psycopg = pytest.importorskip("psycopg")


def test_insert_creates_year_partition(monkeypatch):
    dbname = f"test_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")
    monkeypatch.setenv("DATABASE_URL", f"postgresql+psycopg://root@/{dbname}")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    conn = connect_db()
    insert_release(
        conn,
        "foo",
        CATEGORY_MAP["movies"],
        None,
        None,
        None,
        None,
        "2025-02-01T00:00:00+00:00",
    )
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_class WHERE relname='release_movies_2025'")
    assert cur.fetchone() is not None
    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
