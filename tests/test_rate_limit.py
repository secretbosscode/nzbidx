from __future__ import annotations

import asyncio
import concurrent.futures

from nzbidx_api import rate_limit as rl  # type: ignore


def test_in_memory_rate_limiter_counts() -> None:
    """RateLimiter should track counts per key."""

    limiter = rl.RateLimiter(limit=5, window=60)
    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2
    assert asyncio.run(limiter.increment("5.6.7.8")) == 1


def test_parallel_increment_counts() -> None:
    """Concurrent increments should produce sequential counts."""

    limiter = rl.RateLimiter(limit=1000, window=60)

    def call_increment() -> int:
        return asyncio.run(limiter.increment("1.2.3.4"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(lambda _: call_increment(), range(100)))

    assert sorted(results) == list(range(1, 101))
    assert asyncio.run(limiter.increment("1.2.3.4")) == 101


def test_window_resets_counts(monkeypatch) -> None:
    """Counts reset when a new time window begins."""

    current = 0.0

    def fake_monotonic() -> float:
        return current

    monkeypatch.setattr(rl.time, "monotonic", fake_monotonic)

    limiter = rl.RateLimiter(limit=5, window=10)
    assert asyncio.run(limiter.increment("1.2.3.4")) == 1

    current = 5.0
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2

    current = 15.0
    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
