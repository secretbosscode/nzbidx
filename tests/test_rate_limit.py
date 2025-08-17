"""Tests for the rate limiting middleware."""

from __future__ import annotations

from nzbidx_api import rate_limit as rl


class _FailingRedis:
    """Redis stand-in that raises on increment."""

    @classmethod
    def from_url(cls, url: str) -> "_FailingRedis":  # pragma: no cover - simple
        return cls()

    def incr(self, key: str) -> int:  # pragma: no cover - simple
        from redis.exceptions import ResponseError

        raise ResponseError("boom")


def test_falls_back_to_memory(monkeypatch) -> None:
    """Failures talking to Redis should degrade to in-memory tracking."""

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(rl, "Redis", _FailingRedis)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is True

    assert limiter.increment("1.2.3.4") == 1
    assert limiter.use_redis is False
    assert limiter.increment("1.2.3.4") == 2


class _ExpireFailRedis:
    """Redis stand-in that raises on expire."""

    def __init__(self) -> None:
        self.count = 0

    @classmethod
    def from_url(cls, url: str) -> "_ExpireFailRedis":  # pragma: no cover - simple
        return cls()

    def incr(self, key: str) -> int:  # pragma: no cover - simple
        self.count += 1
        return self.count

    def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - simple
        from redis.exceptions import ResponseError

        raise ResponseError("boom")


def test_expire_fallback(monkeypatch) -> None:
    """Failures calling expire should degrade to in-memory tracking."""

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(rl, "Redis", _ExpireFailRedis)

    limiter = rl.RateLimiter(limit=10, window=60)

    assert limiter.use_redis is True

    assert limiter.increment("1.2.3.4") == 1
    assert limiter.use_redis is False
    assert limiter.increment("1.2.3.4") == 2
