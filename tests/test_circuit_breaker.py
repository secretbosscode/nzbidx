from __future__ import annotations
import asyncio
import pytest

from nzbidx_api.middleware_circuit import (
    CircuitBreaker,
    CircuitOpenError,
    call_with_retry_async,
)


def test_circuit_breaker_async_locking() -> None:
    breaker = CircuitBreaker(max_failures=1, reset_seconds=60)
    count_lock = asyncio.Lock()
    call_count = 0

    async def fail() -> None:
        nonlocal call_count
        async with count_lock:
            call_count += 1
        await asyncio.sleep(0.05)
        raise ValueError("boom")

    async def worker() -> None:
        with pytest.raises((ValueError, CircuitOpenError)):
            await breaker.call(fail)

    async def run() -> None:
        await asyncio.gather(*(worker() for _ in range(5)))
        assert call_count == 1
        assert await breaker.is_open()

    asyncio.run(run())


def test_circuit_breaker_async_concurrency(monkeypatch) -> None:
    monkeypatch.setattr("nzbidx_api.middleware_circuit.settings.retry_max", 0)

    breaker = CircuitBreaker(max_failures=1, reset_seconds=60)
    count_lock = asyncio.Lock()
    call_count = 0

    async def fail_async() -> None:
        nonlocal call_count
        async with count_lock:
            call_count += 1
        raise ValueError("boom")

    async def worker() -> None:
        with pytest.raises((ValueError, CircuitOpenError)):
            await call_with_retry_async(breaker, "dep", fail_async)

    async def run() -> None:
        await asyncio.gather(*(worker() for _ in range(10)))
        assert call_count == 1
        assert await breaker.is_open()

    asyncio.run(run())
