"""Tests for query safety and OpenSearch error handling."""

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


def test_long_query_rejected(monkeypatch) -> None:
    resp = asyncio.run(main.api(DummyRequest(b"t=search&q=" + b"a" * 257)))
    assert resp.status_code == 400


def test_opensearch_timeout(monkeypatch, caplog) -> None:
    class Boom:
        def search(self, *args, **kwargs):  # pragma: no cover - raised
            raise RuntimeError("timeout")

    monkeypatch.setattr(main, "opensearch", Boom())
    with caplog.at_level("WARNING"):
        resp = asyncio.run(main.api(DummyRequest(b"t=search&q=test")))
    assert resp.status_code == 200
    assert caplog.records
