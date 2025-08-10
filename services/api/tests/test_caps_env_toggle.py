"""Caps endpoint reflects environment configuration."""

from pathlib import Path
import sys
import asyncio

# Ensure importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import api  # noqa: E402


class DummyRequest:
    def __init__(self, query: bytes):
        from urllib.parse import parse_qs

        self.query_params = {k: v[0] for k, v in parse_qs(query.decode()).items()}
        self.headers = {}


def test_caps_respects_env(monkeypatch):
    monkeypatch.setenv("MOVIES_CAT_ID", "1234")
    monkeypatch.setenv("ALLOW_XXX", "true")
    monkeypatch.setenv("SAFESEARCH", "off")
    body = asyncio.run(api(DummyRequest(b"t=caps"))).body.decode()
    assert '<category id="1234" name="Movies"/>' in body
    assert "XXX/Adult" in body

    monkeypatch.setenv("ALLOW_XXX", "false")
    body = asyncio.run(api(DummyRequest(b"t=caps"))).body.decode()
    assert "XXX/Adult" not in body
