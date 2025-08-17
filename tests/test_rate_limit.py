"""Tests for the rate limiting middleware."""

from __future__ import annotations

import asyncio

from nzbidx_api import rate_limit as rl


class _FailingRedis:
    """Redis stand-in that raises on increment."""

    @classmethod
    def from_url(cls, url: str) -> "_FailingRedis":  # pragma: no cover - simple
        return cls()

    async def incr(self, key: str) -> int:  # pragma: no cover - simple
        from redis.exceptions import ResponseError

        raise ResponseError("boom")


def test_falls_back_to_memory(monkeypatch) -> None:
    """Failures talking to Redis should degrade to in-memory tracking."""

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(rl, "Redis", _FailingRedis)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is True

    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert limiter.use_redis is False
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2


class _ExpireFailRedis:
    """Redis stand-in that raises on expire."""

    def __init__(self) -> None:
        self.count = 0

    @classmethod
    def from_url(cls, url: str) -> "_ExpireFailRedis":  # pragma: no cover - simple
        return cls()

    async def incr(self, key: str) -> int:  # pragma: no cover - simple
        self.count += 1
        return self.count

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - simple
        from redis.exceptions import ResponseError

        raise ResponseError("boom")


def test_expire_fallback(monkeypatch) -> None:
    """Failures calling expire should degrade to in-memory tracking."""

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(rl, "Redis", _ExpireFailRedis)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is True

    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert limiter.use_redis is False
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2


class _SuccessRedis:
    """Redis stand-in that succeeds."""

    def __init__(self) -> None:
        self.store: dict[str, int] = {}
        self.expire_args: list[tuple[str, int]] = []

    @classmethod
    def from_url(cls, url: str) -> "_SuccessRedis":  # pragma: no cover - simple
        inst = cls()
        cls.instance = inst
        return inst

    async def incr(self, key: str) -> int:  # pragma: no cover - simple
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - simple
        self.expire_args.append((key, ttl))


def test_redis_success(monkeypatch) -> None:
    """Successful Redis operations stay Redis-backed."""

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(rl, "Redis", _SuccessRedis)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is True

    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2
    assert len(_SuccessRedis.instance.expire_args) == 1


def test_in_memory(monkeypatch) -> None:
    """When Redis is unavailable, use in-memory tracking."""

    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setattr(rl, "Redis", None)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is False

    assert asyncio.run(limiter.increment("1.2.3.4")) == 1
    assert asyncio.run(limiter.increment("1.2.3.4")) == 2
