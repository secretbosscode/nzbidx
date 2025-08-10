"""Tests for the caps endpoint."""

from pathlib import Path
import sys
from urllib.parse import parse_qs
import asyncio

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.main import api  # noqa: E402
from nzbidx_api.newznab import adult_disabled_xml  # noqa: E402


class DummyRequest:
    def __init__(self, query_string: bytes):
        self.query_params = {
            k: v[0] for k, v in parse_qs(query_string.decode()).items()
        }
        self.headers = {}


def test_caps() -> None:
    """``/api?t=caps`` should return caps XML."""
    request = DummyRequest(b"t=caps")
    response = asyncio.run(api(request))
    body = response.body.decode()
    assert response.status_code == 200
    assert "<caps>" in body
    assert '<category id="2000" name="Movies"/>' in body


def test_caps_hides_xxx_with_safesearch(monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_XXX", "true")
    monkeypatch.setenv("SAFESEARCH", "on")
    request = DummyRequest(b"t=caps")
    body = asyncio.run(api(request)).body.decode()
    assert "XXX/Adult" not in body


def test_caps_hides_xxx_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_XXX", "false")
    monkeypatch.setenv("SAFESEARCH", "off")
    request = DummyRequest(b"t=caps")
    body = asyncio.run(api(request)).body.decode()
    assert "XXX/Adult" not in body


def test_search_adult_disabled(monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_XXX", "false")
    request = DummyRequest(b"t=search&cat=6000")
    response = asyncio.run(api(request))
    assert response.body.decode() == adult_disabled_xml()
