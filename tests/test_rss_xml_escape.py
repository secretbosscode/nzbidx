"""Tests for XML escaping in RSS feed generation."""

from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import sys
from contextlib import nullcontext
from pathlib import Path
from xml.etree import ElementTree as ET

# Ensure the API package is importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api.newznab import rss_xml  # type: ignore
from nzbidx_api import search as search_mod  # type: ignore


def test_rss_xml_escapes_special_characters() -> None:
    item = {
        "title": "> free! laptop & < stuff",
        "guid": "> free! laptop & < stuff",
        "pubDate": "",
        "category": "2030",
        "link": "/api?t=getnzb&id=> free! laptop & < stuff",
    }

    xml = rss_xml([item])

    assert "&lt;" in xml and "&gt;" in xml and "&amp;" in xml

    root = ET.fromstring(xml)
    parsed = root.find("./channel/item")
    assert parsed is not None
    assert parsed.findtext("title") == item["title"]
    assert parsed.findtext("guid") == item["guid"]
    assert parsed.findtext("link") == item["link"]


def test_search_releases_encodes_link_id(monkeypatch) -> None:
    class DummyClient:
        def search(self, **kwargs):
            return {
                "hits": {
                    "hits": [
                        {
                            "_id": 'id with "quotes" and spaces',
                            "_source": {
                                "norm_title": "",
                                "posted_at": "",
                                "category": "",
                                "size_bytes": 1,
                            },
                        }
                    ]
                }
            }

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    items = search_mod.search_releases(client, {"must": []}, limit=1)

    link = items[0]["link"]
    assert "%20" in link and "%22" in link
    assert " " not in link and "\"" not in link
