from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
import asyncio

import pytest

from nzbidx_api.middleware_circuit import (
    CircuitBreaker,
    CircuitOpenError,
    call_with_retry_async,
)


def test_circuit_breaker_thread_safety() -> None:
    breaker = CircuitBreaker(max_failures=1, reset_seconds=60)
    count_lock = threading.Lock()
    call_count = 0

    def fail() -> None:
        nonlocal call_count
        with count_lock:
            call_count += 1
        raise ValueError("boom")

    def worker() -> None:
        with pytest.raises((ValueError, CircuitOpenError)):
            breaker.call(fail)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker) for _ in range(10)]
        for future in futures:
            future.result()

    assert call_count == 1
    assert breaker.is_open()


def test_circuit_breaker_async_concurrency(monkeypatch) -> None:
    monkeypatch.setattr("nzbidx_api.middleware_circuit.retry_max", lambda: 0)

    breaker = CircuitBreaker(max_failures=1, reset_seconds=60)
    count_lock = threading.Lock()
    call_count = 0

    async def fail_async() -> None:
        nonlocal call_count
        with count_lock:
            call_count += 1
        raise ValueError("boom")

    async def worker() -> None:
        with pytest.raises((ValueError, CircuitOpenError)):
            await call_with_retry_async(breaker, "dep", fail_async)

    async def run() -> None:
        await asyncio.gather(*(worker() for _ in range(10)))

    asyncio.run(run())

    assert call_count == 1
    assert breaker.is_open()
