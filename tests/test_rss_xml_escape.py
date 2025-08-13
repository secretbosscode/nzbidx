"""Tests for XML escaping in RSS feed generation."""

from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import sys
from pathlib import Path
from xml.etree import ElementTree as ET

# Ensure the API package is importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api.newznab import rss_xml  # type: ignore


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
