"""Tests for NZB caching helper."""

from services.api.src.nzbidx_api import newznab


class DummyCache:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def get(self, key: str) -> bytes | None:  # type: ignore[override]
        return self.store.get(key)

    def setex(self, key: str, _ttl: int, value: bytes | str) -> None:  # type: ignore[override]
        if isinstance(value, str):
            value = value.encode("utf-8")
        self.store[key] = value


def test_failed_fetch_cached(monkeypatch):
    cache = DummyCache()
    calls: list[str] = []

    def boom(release_id: str) -> str:
        calls.append(release_id)
        raise RuntimeError("boom")

    monkeypatch.setattr(newznab.nzb_builder, "build_nzb_for_release", boom)

    key = "nzb:123"
    # first call populates failure sentinel
    try:
        newznab.get_nzb("123", cache)
    except newznab.NzbFetchError:
        pass
    assert cache.store[key] == newznab.FAIL_SENTINEL
    assert calls == ["123"]

    calls.clear()
    # second call should hit cache and not invoke builder
    try:
        newznab.get_nzb("123", cache)
    except newznab.NzbFetchError:
        pass
    assert calls == []
