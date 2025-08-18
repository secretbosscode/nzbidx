import sys
from pathlib import Path
import uuid

import psycopg
import pytest

# ruff: noqa: E402 - path manip before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db


def test_connect_db_creates_partitions(monkeypatch):
    """connect_db should create partitioned release table in PostgreSQL."""
    dbname = f"test_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")
    monkeypatch.setenv("DATABASE_URL", f"postgresql+psycopg://root@/{dbname}")

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    try:
        conn = connect_db()
    except Exception as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_partitioned_table WHERE partrelid='release'::regclass")
    assert cur.fetchone() is not None
    cur.execute("SELECT 1 FROM pg_class WHERE relname='release_movies'")
    assert cur.fetchone() is not None
    conn.close()

    try:
        admin = psycopg.connect(dbname="postgres", user="root")
        admin.autocommit = True
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.close()
    except psycopg.OperationalError as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
