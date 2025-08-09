"""Tests for NZB caching using Redis."""

from pathlib import Path
import sys

import fakeredis

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api import newznab  # noqa: E402


def test_get_nzb_uses_cache(monkeypatch) -> None:
    """``get_nzb`` should cache NZB documents in Redis."""
    fake = fakeredis.FakeRedis()
    calls = {"count": 0}

    original = newznab.nzb_xml_stub

    def stub(release_id: str) -> str:
        calls["count"] += 1
        return original(release_id)

    monkeypatch.setattr(newznab, "nzb_xml_stub", stub)

    release_id = "123"
    xml1 = newznab.get_nzb(release_id, fake)
    xml2 = newznab.get_nzb(release_id, fake)

    assert xml1 == xml2
    assert fake.get(f"nzb:{release_id}").decode() == xml1
    assert calls["count"] == 1
