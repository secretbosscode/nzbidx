from __future__ import annotations

from xml.etree import ElementTree as ET

from nzbidx_api.newznab import rss_xml  # type: ignore


def test_rss_xml_includes_enclosure() -> None:
    item = {
        "title": "t",
        "guid": "1",
        "pubDate": "Mon, 01 Jan 2024 00:00:00 +0000",
        "category": "2030",
        "link": "/api?t=getnzb&id=1",
        "size": "123",
    }
    xml = rss_xml([item])
    root = ET.fromstring(xml)
    enclosure = root.find("./channel/item/enclosure")
    assert enclosure is not None
    assert enclosure.get("url") == item["link"]
    assert enclosure.get("type") == "application/x-nzb"
    assert enclosure.get("length") == item["size"]
    assert not root.findall("./channel/item/attr")


def test_rss_xml_extended_attrs() -> None:
    item = {
        "title": "t",
        "guid": "1",
        "pubDate": "Mon, 01 Jan 2024 00:00:00 +0000",
        "category": "2030",
        "link": "/api?t=getnzb&id=1",
        "size": "123",
        "imdbid": "tt1234567",
    }
    xml = rss_xml([item], extended=True)
    root = ET.fromstring(xml)
    attrs = root.findall("./channel/item/attr")
    assert any(a.get("name") == "imdbid" and a.get("value") == item["imdbid"] for a in attrs)
    assert any(a.get("name") == "size" and a.get("value") == item["size"] for a in attrs)
    assert any(a.get("name") == "category" and a.get("value") == item["category"] for a in attrs)
