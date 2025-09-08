from pathlib import Path


def test_schema_sql_present() -> None:
    sql_path = Path(__file__).resolve().parents[1] / "db" / "init" / "schema.sql"
    sql = sql_path.read_text()
    assert "CREATE TABLE IF NOT EXISTS release" in sql
    assert "posted_at TIMESTAMPTZ" in sql
    assert (
        "CREATE INDEX IF NOT EXISTS release_search_idx ON release USING GIN (search_vector);"
        in sql
    )
