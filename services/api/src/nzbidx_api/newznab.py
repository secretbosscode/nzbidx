"""Helpers for the Newznab API."""


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    return (
        '<caps><server version="0.1" title="nzbidx"/>'
        '<limits max="100" default="50"/></caps>'
    )


def rss_xml(items: list[dict[str, str]]) -> str:
    """Return a simple RSS feed with the provided items.

    Each ``item`` dict should contain ``title``, ``guid``, ``pubDate``,
    ``category`` and ``link`` keys. No escaping is performed as the values are
    expected to be safe for XML.
    """

    items_xml = "".join(
        "<item>"
        f"<title>{i['title']}</title>"
        f"<guid>{i['guid']}</guid>"
        f"<pubDate>{i['pubDate']}</pubDate>"
        f"<category>{i['category']}</category>"
        f"<link>{i['link']}</link>"
        "</item>"
        for i in items
    )
    return f'<rss version="2.0"><channel>{items_xml}</channel></rss>'


def nzb_xml_stub(release_id: str) -> str:
    """Return a minimal NZB document for the given ``release_id``."""

    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">'
        "<file>"
        "<segments>"
        '<segment bytes="0" number="1">dummy@message.id</segment>'
        "</segments>"
        "</file>"
        "</nzb>"
    )
