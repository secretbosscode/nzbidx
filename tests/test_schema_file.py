from importlib import resources


def test_schema_sql_present() -> None:
    sql = resources.read_text("nzbidx_api", "schema.sql")
    assert "CREATE TABLE IF NOT EXISTS release" in sql
