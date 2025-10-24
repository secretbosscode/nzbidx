import uuid
from datetime import date

import pytest
from nzbidx_ingest.main import connect_db, CATEGORY_MAP
from nzbidx_ingest.db_migrations import (
    drop_release_partitions_before,
    ensure_release_year_partition,
    migrate_release_partitions_by_date,
)

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
        cur.execute(
            "SELECT 1 FROM pg_class WHERE relname=%s", (f"release_{name}_2024",)
        )
        assert cur.fetchone() is not None
        cur.execute(f"SELECT search_vector FROM release_{name}_2024")
        assert cur.fetchone()[0] is not None

    cur.close()
    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")


def test_drop_release_partitions_before_trims_partial_year(monkeypatch):
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

    migrate_release_partitions_by_date(conn, "movies")
    ensure_release_year_partition(conn, "movies", 2023)
    ensure_release_year_partition(conn, "movies", 2024)

    category_id = int(CATEGORY_MAP["movies"])
    for title, posted_at in [
        ("old-2023", "2023-03-10T00:00:00+00:00"),
        ("trim-2024", "2024-01-15T00:00:00+00:00"),
        ("keep-2024", "2024-12-15T00:00:00+00:00"),
    ]:
        cur.execute(
            "INSERT INTO release (norm_title, category, category_id, posted_at) VALUES (%s, %s, %s, %s)",
            (title, str(category_id), category_id, posted_at),
        )

    result = drop_release_partitions_before(conn, date(2024, 10, 1))

    dropped = result["dropped"]
    deleted = result["deleted"]
    assert "release_movies_2023" in dropped
    assert deleted.get("release_movies_2024") == 1

    cur.execute("SELECT to_regclass(%s)", ("release_movies_2023",))
    assert cur.fetchone()[0] is None

    cur.execute("SELECT COUNT(*) FROM release_movies_2024")
    assert cur.fetchone()[0] == 1

    cur.close()
    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
