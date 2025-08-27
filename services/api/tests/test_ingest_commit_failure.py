import logging

import pytest

import nzbidx_ingest.ingest_loop as loop  # type: ignore


class FailingDB:
    """Database stub whose commit always fails."""

    __module__ = "sqlite3"

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    def cursor(self):  # pragma: no cover - trivial
        return self.Cursor()

    def commit(self) -> None:  # pragma: no cover - trivial
        raise RuntimeError("commit failed")


class DummyClient:
    def group(self, group: str):  # pragma: no cover - trivial
        return ("211", "1", "1", "1", group)

    def xover(self, group: str, start: int, end: int):  # pragma: no cover - trivial
        return [{"bytes": "0"}]


def test_commit_failure_logged_and_propagated(monkeypatch, caplog) -> None:
    monkeypatch.setattr(loop.cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(loop.cursors, "set_cursor", lambda _g, _c: None)

    db = FailingDB()
    client = DummyClient()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="commit failed"):
            loop._process_groups(client, db, ["alt.test"], set())

    assert any("ingest_commit_error" in record.message for record in caplog.records)
