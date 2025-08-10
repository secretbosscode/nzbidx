"""Tests for Cache-Control and ETag handling."""

from pathlib import Path
import sys
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes, headers: dict[str, str] | None = None):
        from urllib.parse import parse_qs

        self.query_params = {
            k: v[0] for k, v in parse_qs(query_string.decode()).items()
        }
        self.headers = headers or {}


def test_cache_headers_and_etag() -> None:
    req1 = DummyRequest(b"t=search&q=test")
    resp1 = asyncio.run(main.api(req1))
    etag = resp1.headers.get("ETag")
    assert etag
    assert "cache-control" in {k.lower() for k in resp1.headers}

    req2 = DummyRequest(b"t=search&q=test", headers={"If-None-Match": etag})
    resp2 = asyncio.run(main.api(req2))
    assert resp2.status_code == 304

    req3 = DummyRequest(
        b"t=search&q=test",
        headers={"If-None-Match": etag, "Cache-Control": "no-cache"},
    )
    resp3 = asyncio.run(main.api(req3))
    assert resp3.status_code == 200
