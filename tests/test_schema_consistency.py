import asyncio
from nzbidx_api import db
from nzbidx_ingest import main


def test_api_and_ingest_schema_identical(monkeypatch):
    api_stmts = []
    ingest_stmts = []

    class AsyncConn:
        async def execute(self, stmt):
            if (
                not str(stmt).lstrip().upper().startswith("SELECT")
                and str(stmt) != "ALTER ROLE CURRENT_USER NOSUPERUSER"
            ):
                api_stmts.append(str(stmt))

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def scalar(self, stmt, params=None):
            return 1

    class AsyncEngine:
        def connect(self):
            class Ctx:
                async def __aenter__(self_inner):
                    return AsyncConn()

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return None

            return Ctx()

    monkeypatch.setattr(db, "get_engine", lambda: AsyncEngine())
    monkeypatch.setattr(db, "text", lambda s: s)
    monkeypatch.setattr(db, "DATABASE_URL", "postgresql+asyncpg://u@h/db")
    asyncio.run(db.apply_schema())

    class DummyResult:
        def __init__(self, val=False):
            self.val = val

        def fetchone(self):
            return [self.val]

    class SyncConn:
        def execute(self, stmt):
            if str(stmt).lstrip().upper().startswith("SELECT"):
                return DummyResult(False)
            ingest_stmts.append(str(stmt))
            return DummyResult()

        def commit(self):
            return None

        def rollback(self):
            return None

    class Engine:
        def connect(self):
            class Ctx:
                def __enter__(self_inner):
                    return SyncConn()

                def __exit__(self_inner, exc_type, exc, tb):
                    return None

            return Ctx()

        def raw_connection(self):
            class Raw:
                def cursor(self):
                    class Cur:
                        def execute(self, *a, **k):
                            return None

                        def fetchone(self):
                            return [0]

                    return Cur()

                def close(self):
                    return None

            return Raw()

    monkeypatch.setenv("DATABASE_URL", "postgres://u@h/db")
    monkeypatch.setattr(main, "create_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(main, "text", lambda s: s)
    main.connect_db()

    assert api_stmts == ingest_stmts
