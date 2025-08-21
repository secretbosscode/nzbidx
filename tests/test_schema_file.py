from importlib.resources import files


def test_schema_sql_present() -> None:
    sql = (files("nzbidx_api") / "schema.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS release" in sql
    assert "posted_at TIMESTAMPTZ" in sql
    assert (
        "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at);"
        in sql
    )
