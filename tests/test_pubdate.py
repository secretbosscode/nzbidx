from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import sys
from pathlib import Path
from contextlib import nullcontext
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

# Ensure API package importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

import nzbidx_api.search as search_mod  # type: ignore
from nzbidx_api.newznab import rss_xml  # type: ignore


def test_rss_xml_includes_channel_pubdate() -> None:
    item = {
        "title": "test",
        "guid": "1",
        "pubDate": "Mon, 01 Jan 2024 00:00:00 +0000",
        "category": "2030",
        "link": "/api?t=getnzb&id=1",
    }
    xml = rss_xml([item])
    root = ET.fromstring(xml)
    pub = root.findtext("./channel/pubDate")
    assert pub is not None
    assert parsedate_to_datetime(pub) is not None


def test_search_releases_formats_pubdate_and_includes_size(monkeypatch) -> None:
    iso_date = "2024-01-01T00:00:00Z"

    class DummyClient:
        def search(self, **kwargs):
            return {
                "hits": {
                    "hits": [
                        {
                            "_id": "abc",
                            "_source": {
                                "norm_title": "Test",
                                "posted_at": iso_date,
                                "category": "2030",
                                "size_bytes": 123,
                            },
                        }
                    ]
                }
            }

    def dummy_call_with_retry(_breaker, _dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    items = search_mod.search_releases(client, {"must": []}, limit=1)
    assert parsedate_to_datetime(items[0]["pubDate"]) is not None
    assert items[0]["size"] == "123"
