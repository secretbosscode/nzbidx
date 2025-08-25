"""Tests for ``build_nzb_for_release`` segment handling."""

from __future__ import annotations

import json

import pytest

from nzbidx_api import nzb_builder, newznab


class DummyCursor:
    def __init__(self, seg_data: str) -> None:
        self.seg_data = seg_data

    def __enter__(self):  # type: ignore[override]
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
        pass

    def execute(self, _sql, _params):  # type: ignore[override]
        pass

    def fetchone(self):  # type: ignore[override]
        return (self.seg_data,)


class DummyConn:
    def __init__(self, seg_data: str) -> None:
        self.seg_data = seg_data

    def cursor(self):  # type: ignore[override]
        return DummyCursor(self.seg_data)


DummyConn.__module__ = "sqlite3"


@pytest.mark.parametrize(
    "seg_data",
    [
        json.dumps([{"number": 1, "message_id": "m1", "group": "g", "size": 123}]),
        json.dumps([[1, "m1", "g", 123]]),
    ],
)
def test_build_nzb_for_release_accepts_segment_formats(monkeypatch, seg_data):
    monkeypatch.setattr(nzb_builder, "get_connection", lambda: DummyConn(seg_data))

    xml = nzb_builder.build_nzb_for_release("123")

    assert '<segment bytes="123" number="1">m1</segment>' in xml


@pytest.mark.parametrize(
    "seg_data",
    [
        json.dumps([[1, "m1", "g"]]),
        json.dumps([{"number": "x", "message_id": "m1", "group": "g", "size": 1}]),
        json.dumps([123]),
    ],
)
def test_build_nzb_for_release_invalid_segments(monkeypatch, seg_data):
    monkeypatch.setattr(nzb_builder, "get_connection", lambda: DummyConn(seg_data))

    with pytest.raises(newznab.NzbFetchError, match="invalid segment entry"):
        nzb_builder.build_nzb_for_release("123")
