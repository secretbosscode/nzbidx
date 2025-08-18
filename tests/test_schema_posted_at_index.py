import sys
from pathlib import Path

# Ensure local modules are importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.main import connect_db  # type: ignore


def test_posted_at_column_and_index(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", str(db_path))
    conn = connect_db()
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info('release')").fetchall()]
        assert "posted_at" in cols
        indexes = [row[1] for row in conn.execute("PRAGMA index_list('release')").fetchall()]
        assert "release_posted_at_idx" in indexes
    finally:
        conn.close()
