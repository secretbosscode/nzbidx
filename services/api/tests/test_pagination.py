"""Tests for pagination and limit enforcement."""

from pathlib import Path
import sys
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes):
        from urllib.parse import parse_qs

        self.query_params = {
            k: v[0] for k, v in parse_qs(query_string.decode()).items()
        }
        self.headers: dict[str, str] = {}


def test_limit_and_offset(monkeypatch) -> None:
    calls: list[tuple[int, int]] = []

    def fake_search(client, query, *, limit: int, offset: int) -> list[dict]:
        calls.append((limit, offset))
        return []

    monkeypatch.setattr(main, "opensearch", object())
    monkeypatch.setattr(main, "search_releases", fake_search)

    asyncio.run(main.api(DummyRequest(b"t=search&q=test")))
    asyncio.run(main.api(DummyRequest(b"t=search&q=test&limit=20&offset=5")))

    assert calls[0] == (50, 0)
    assert calls[1] == (20, 5)


def test_limit_above_max(monkeypatch) -> None:
    monkeypatch.setattr(main, "opensearch", object())
    resp = asyncio.run(main.api(DummyRequest(b"t=search&q=test&limit=500")))
    assert resp.status_code == 400
