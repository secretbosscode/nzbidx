from __future__ import annotations

import json
import logging

from nzbidx_api import nzb_builder


class _FakeCursor:
    def __init__(self, seg_data):
        self._seg_data = seg_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        pass

    def fetchone(self):
        return (self._seg_data,)


class _FakeConn:
    def __init__(self, seg_data):
        self._seg_data = seg_data

    def cursor(self):
        return _FakeCursor(self._seg_data)


# make connection appear as sqlite3 for placeholder detection
_FakeConn.__module__ = "sqlite3"


def _patch_conn(monkeypatch, seg_data):
    monkeypatch.setattr(nzb_builder, "get_connection", lambda: _FakeConn(seg_data))


def test_segments_from_db_dict(monkeypatch):
    data = [{"number": 1, "message_id": "m1", "group": "g", "size": 123}]
    seg_data = json.dumps(data)
    _patch_conn(monkeypatch, seg_data)
    assert nzb_builder._segments_from_db(1) == [(1, "m1", "g", 123)]


def test_segments_from_db_list(monkeypatch):
    data = [[1, "m1", "g", 123], [2, "m2", "g", 456]]
    seg_data = json.dumps(data)
    _patch_conn(monkeypatch, seg_data)
    assert nzb_builder._segments_from_db(1) == [
        (1, "m1", "g", 123),
        (2, "m2", "g", 456),
    ]


def test_segments_from_db_malformed_sequence(monkeypatch, caplog):
    data = [[1, "m1"], [2, "m2", "g", 456]]
    seg_data = json.dumps(data)
    _patch_conn(monkeypatch, seg_data)
    with caplog.at_level(logging.WARNING, logger="nzbidx_api.nzb_builder"):
        segments = nzb_builder._segments_from_db(1)
    assert segments == [(2, "m2", "g", 456)]
    assert "malformed_segment_length" in caplog.messages
