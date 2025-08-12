"""Tests for Redis cache startup."""

from __future__ import annotations

from nzbidx_api import main


class _FakeRedis:
    calls: list[tuple[str, str]] = []

    @classmethod
    def from_url(cls, url: str):
        return cls()

    def ping(self) -> None:  # pragma: no cover - simple
        pass

    def config_set(self, key: str, value: str) -> None:
        self.calls.append((key, value))

    def close(self) -> None:  # pragma: no cover - simple
        pass


def test_init_cache_disables_persistence(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("REDIS_DISABLE_PERSISTENCE", "1")
    monkeypatch.setattr(main, "Redis", _FakeRedis)
    _FakeRedis.calls = []
    main.cache = None
    main.init_cache()
    assert ("save", "") in _FakeRedis.calls
    assert ("appendonly", "no") in _FakeRedis.calls
    main.cache = None
