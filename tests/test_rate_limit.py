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
