from nzbidx_ingest.db_migrations import drop_unused_release_adult_partitions


def test_drop_unused_release_adult_partitions(monkeypatch):
    tables = {
        "release_adult_keep": 0,
        "release_adult_drop": 0,
        "release_adult_used": 1,
    }

    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn
            self._result = []

        def execute(self, stmt, params=None):
            sql = " ".join(stmt.split())
            if "FROM pg_inherits" in sql:
                self._result = [(name,) for name in self.conn.tables]
            elif sql.startswith("SELECT 1 FROM"):
                table = sql.split()[3]
                count = self.conn.tables.get(table, 0)
                self._result = [(1,)] if count > 0 else []
            elif sql.startswith("DROP TABLE"):
                table = sql.split()[2]
                self.conn.dropped.append(table)
                self._result = []
            else:
                self._result = []

        def fetchall(self):
            return self._result

        def fetchone(self):
            return self._result[0] if self._result else None

        def close(self):
            pass

    class DummyConn:
        def __init__(self, tables):
            self.tables = tables
            self.dropped: list[str] = []

        def cursor(self):
            return DummyCursor(self)

        def commit(self):
            return None

    monkeypatch.setenv("RELEASE_ADULT_PARTITIONS_RETAIN", "release_adult_keep")

    conn = DummyConn(tables)
    drop_unused_release_adult_partitions(conn)

    assert conn.dropped == ["release_adult_drop"]
