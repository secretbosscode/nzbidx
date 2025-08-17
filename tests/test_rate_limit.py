from __future__ import annotations

import asyncio

from nzbidx_api import rate_limit as rl  # type: ignore


def test_in_memory_rate_limiter_counts() -> None:
    """RateLimiter should track counts per key without Redis."""

    limiter = rl.RateLimiter(limit=5, window=60)
    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2
    assert asyncio.run(limiter.increment("5.6.7.8")) == 1
