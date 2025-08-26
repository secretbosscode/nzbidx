from __future__ import annotations

import asyncio
import concurrent.futures

from nzbidx_api import rate_limit as rl  # type: ignore


def test_in_memory_rate_limiter_counts() -> None:
    """RateLimiter should track counts per key."""

    limiter = rl.RateLimiter(limit=5, window=60, max_entries=100)
    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2
    assert asyncio.run(limiter.increment("5.6.7.8")) == 1


def test_parallel_increment_counts() -> None:
    """Concurrent increments should produce sequential counts."""

    limiter = rl.RateLimiter(limit=1000, window=60, max_entries=1000)

    def call_increment() -> int:
        return asyncio.run(limiter.increment("1.2.3.4"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(lambda _: call_increment(), range(100)))

    assert sorted(results) == list(range(1, 101))
    assert asyncio.run(limiter.increment("1.2.3.4")) == 101


def test_lru_eviction() -> None:
    """Limiter should evict least recently used IPs when full."""

    limiter = rl.RateLimiter(limit=5, window=60, max_entries=2)
    asyncio.run(limiter.increment("1.1.1.1"))
    asyncio.run(limiter.increment("2.2.2.2"))
    # Third unique IP should evict the least recently used (1.1.1.1)
    asyncio.run(limiter.increment("3.3.3.3"))
    assert "1.1.1.1" not in limiter.counts
    assert asyncio.run(limiter.increment("2.2.2.2")) == 2
