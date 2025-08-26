from __future__ import annotations

from nzbidx_api import backfill_release_parts as backfill_mod


class DummyClient:
    def __init__(self) -> None:
        self.called: tuple[int, int] | None = None

    def high_water_mark(self, group: str) -> int:
        assert group == "alt.test"
        return 100

    def xover(self, group: str, start: int, end: int):
        self.called = (start, end)
        return []

    def body_size(self, message_id: str) -> int:  # pragma: no cover - unused
        return 0


def test_fetch_segments_limits_xover_range(monkeypatch) -> None:
    client = DummyClient()
    monkeypatch.setattr(backfill_mod, "XOVER_LOOKBACK", 10)

    backfill_mod._fetch_segments("my release", "alt.test", client)
    assert client.called == (91, 100)
