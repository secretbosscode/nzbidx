from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import backfill_release_parts as backfill_mod  # noqa: E402


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
    monkeypatch.setattr(backfill_mod, "NNTPClient", lambda: client)
    monkeypatch.setattr(backfill_mod, "XOVER_LOOKBACK", 10)

    backfill_mod._fetch_segments("my release", "alt.test")
    assert client.called == (91, 100)
