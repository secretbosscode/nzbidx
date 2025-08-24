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
    assert any(
        a.get("name") == "imdbid" and a.get("value") == item["imdbid"] for a in attrs
    )
    assert any(
        a.get("name") == "size" and a.get("value") == item["size"] for a in attrs
    )
    assert any(
        a.get("name") == "category" and a.get("value") == item["category"]
        for a in attrs
    )


def test_rss_xml_channel_metadata() -> None:
    item = {
        "title": "t",
        "guid": "1",
        "pubDate": "Mon, 01 Jan 2024 00:00:00 +0000",
        "category": "2030",
        "link": "/api?t=getnzb&id=1",
    }
    xml = rss_xml([item], feed_url="http://example.com/feed", language="en-US")
    root = ET.fromstring(xml)
    channel = root.find("./channel")
    assert channel is not None
    children = [child.tag for child in list(channel)]
    assert children.index("title") < children.index("pubDate")
    assert children.index("link") < children.index("pubDate")
    assert children.index("description") < children.index("pubDate")
    assert channel.find("./title").text == "nzbidx"
    assert channel.find("./link").text == "/"
    assert channel.find("./description").text == "nzbidx RSS feed"
    lang = channel.find("./language")
    assert lang is not None and lang.text == "en-US"
    atom_link = channel.find("{http://www.w3.org/2005/Atom}link")
    assert atom_link is not None
    assert atom_link.get("href") == "http://example.com/feed"
    assert atom_link.get("rel") == "self"
    assert atom_link.get("type") == "application/rss+xml"
