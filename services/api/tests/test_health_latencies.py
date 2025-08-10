"""Tests for latency fields in health endpoint."""

from pathlib import Path
import sys
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyRequest:
    def __init__(self):
        self.query_params = {}


def test_health_latencies(monkeypatch) -> None:
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

    times = iter([0.0, 0.0, 0.005, 0.005, 0.010, 0.010])

    def fake_monotonic():
        try:
            return next(times)
        except StopIteration:
            return 0.010

    monkeypatch.setattr(main.time, "monotonic", fake_monotonic)

    resp = asyncio.run(main.health(DummyRequest()))
    import json

    payload = json.loads(resp.body)
    assert payload["os"] == "ok"
    assert payload["redis"] == "ok"
    assert payload["os_latency_ms"] == 5
    assert payload["redis_latency_ms"] == 5
