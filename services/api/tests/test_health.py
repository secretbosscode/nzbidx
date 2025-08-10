"""Tests for the API health endpoint."""

import json
from pathlib import Path
import sys
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyRequest:
    def __init__(self):
        self.query_params = {}


def test_health_all_ok(monkeypatch) -> None:
    class OS:
        def info(self):
            return {}

    class Redis:
        def ping(self):
            return True

    monkeypatch.setattr(main, "opensearch", OS())
    monkeypatch.setattr(main, "cache", Redis())

    async def ok_ping():
        return True

    monkeypatch.setattr(main, "ping", ok_ping)
    resp = asyncio.run(main.health(DummyRequest()))
    payload = json.loads(resp.body)
    assert payload["db"] == "ok"
    assert payload["os"] == "ok"
    assert payload["redis"] == "ok"


def test_health_all_down(monkeypatch) -> None:
    class OS:
        def info(self):
            raise RuntimeError("boom")

    class Redis:
        def ping(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(main, "opensearch", OS())
    monkeypatch.setattr(main, "cache", Redis())

    async def bad_ping():
        return False

    monkeypatch.setattr(main, "ping", bad_ping)
    resp = asyncio.run(main.health(DummyRequest()))
    payload = json.loads(resp.body)
    assert payload["db"] == "down"
    assert payload["os"] == "down"
    assert payload["redis"] == "down"
