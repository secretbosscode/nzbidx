from __future__ import annotations

import asyncio
import time

import pytest

from nzbidx_api.middleware_circuit import CircuitBreaker, call_with_retry_async


def test_retry_forever_eventual_success(monkeypatch) -> None:
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_max", 0)
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_base_ms", 1)
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_jitter_ms", 0)
    monkeypatch.setattr(
        "nzbidx_api.middleware_circuit.settings.retry_forever_max_ms", 2
    )
    monkeypatch.setattr(
        "nzbidx_api.middleware_circuit.settings.retry_forever_deps", {"dep"}
    )

    breaker = CircuitBreaker(max_failures=1, reset_seconds=0.01)
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("boom")
        return "ok"

    async def run() -> None:
        result = await asyncio.wait_for(
            call_with_retry_async(breaker, "dep", flaky), timeout=1
        )
        assert result == "ok"
        assert attempts == 3

    asyncio.run(run())


def test_retry_forever_circuit_reset(monkeypatch) -> None:
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_max", 0)
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_base_ms", 1)
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_jitter_ms", 0)
    monkeypatch.setattr(
        "nzbidx_api.middleware_circuit.settings.retry_forever_max_ms", 2
    )
    monkeypatch.setattr(
        "nzbidx_api.middleware_circuit.settings.retry_forever_deps", {"dep"}
    )

    breaker = CircuitBreaker(max_failures=1, reset_seconds=0.05)
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("boom")
        return "ok"

    async def run() -> None:
        start = time.monotonic()
        result = await asyncio.wait_for(
            call_with_retry_async(breaker, "dep", flaky), timeout=2
        )
        elapsed = time.monotonic() - start
        assert result == "ok"
        assert attempts == 3
        assert breaker.state() == "closed"
        assert elapsed >= 0.1

    asyncio.run(run())
