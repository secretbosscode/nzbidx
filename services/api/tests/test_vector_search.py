from pathlib import Path
import asyncio
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import db as db_mod  # type: ignore  # noqa: E402


class DummyResult:
    def __iter__(self):
        return iter([{"id": 1, "title": "foo", "category": None, "language": None}])


class DummyConn:
    captured_stmt = ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, stmt, params):
        DummyConn.captured_stmt = str(stmt)
        return DummyResult()


class DummyEngine:
    def connect(self):
        return DummyConn()


def test_similar_releases_queries_by_embedding(monkeypatch):
    monkeypatch.setattr(db_mod, "engine", DummyEngine())
    monkeypatch.setattr(db_mod, "text", lambda s: s)

    result = asyncio.run(db_mod.similar_releases([0.1, 0.2, 0.3], limit=1))

    assert "embedding <->" in DummyConn.captured_stmt
    assert result == [{"id": 1, "title": "foo", "category": None, "language": None}]
