import uuid

import pytest
from nzbidx_ingest.db_migrations import migrate_release_table

psycopg = pytest.importorskip("psycopg")


def test_migrate_release_table_handles_generated_column(monkeypatch):
    dbname = f"test_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    try:
        conn = psycopg.connect(dbname=dbname, user="root")
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    cur.execute(
        """
        CREATE TABLE release (
            id BIGSERIAL,
            norm_title TEXT,
            category TEXT,
            category_id INT,
            language TEXT,
            tags TEXT,
            source_group TEXT,
            size_bytes BIGINT,
            posted_at TIMESTAMPTZ,
            segments JSONB,
            has_parts BOOLEAN,
            part_count INT,
            search_vector tsvector GENERATED ALWAYS AS (
                to_tsvector('simple', coalesce(norm_title,'') || ' ' || coalesce(tags,''))
            ) STORED
        )
        """,
    )
    cur.execute(
        "INSERT INTO release (norm_title, category, category_id, language, tags, posted_at) VALUES (%s, %s, %s, %s, %s, %s)",
        ("test", "2000", 2000, "en", "foo", "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()

    migrate_release_table(conn)

    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM pg_partitioned_table WHERE partrelid='release'::regclass"
    )
    assert cur.fetchone() is not None
    cur.execute("SELECT norm_title, category_id FROM release")
    assert cur.fetchone() == ("test", 2000)

    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
