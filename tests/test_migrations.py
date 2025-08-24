from nzbidx_migrations import _split_sql


def test_split_sql_dollar_do_block():
    sql = "DO $$ BEGIN RAISE NOTICE 'hi'; END $$;"
    assert _split_sql(sql) == [sql]
