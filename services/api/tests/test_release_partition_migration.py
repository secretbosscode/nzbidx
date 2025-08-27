import uuid

import pytest
from nzbidx_ingest.main import connect_db, CATEGORY_MAP
from nzbidx_ingest.db_migrations import migrate_release_partitions_by_date

psycopg = pytest.importorskip("psycopg")


def test_release_partitions_migrate_by_date(monkeypatch):
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
    conn.autocommit = True
    cur = conn.cursor()

    categories = {
        "movies": int(CATEGORY_MAP["movies"]),
        "music": int(CATEGORY_MAP["music"]),
        "tv": int(CATEGORY_MAP["tv"]),
        "books": int(CATEGORY_MAP["books"]),
        "adult": int(CATEGORY_MAP["xxx"]),
        "other": 9000,
    }
    for name, cid in categories.items():
        cur.execute(
            "INSERT INTO release (norm_title, category, category_id, posted_at) VALUES (%s, %s, %s, %s)",
            (name, str(cid), cid, "2024-02-01T00:00:00+00:00"),
        )

    for name in categories:
        migrate_release_partitions_by_date(conn, name)

    for name in categories:
        cur.execute("SELECT 1 FROM pg_class WHERE relname=%s", (f"release_{name}_2024",))
        assert cur.fetchone() is not None

    cur.close()
    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
