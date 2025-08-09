"""Helpers for the Newznab API."""

import os
from typing import Optional

# Optional redis dependency for caching
try:  # pragma: no cover - import guard
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore


MOVIES_CAT = os.getenv("MOVIES_CAT_ID", "2000")
TV_CAT = os.getenv("TV_CAT_ID", "5000")
AUDIO_CAT = os.getenv("AUDIO_CAT_ID", "3000")
BOOKS_CAT = os.getenv("BOOKS_CAT_ID", "7000")
ADULT_CAT = os.getenv("ADULT_CAT_ID", "6000")


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    return (
        '<caps><server version="0.1" title="nzbidx"/>'
        '<limits max="100" default="50"/>'
        "<categories>"
        f'<category id="{MOVIES_CAT}" name="Movies"/>'
        f'<category id="{TV_CAT}" name="TV"/>'
        f'<category id="{AUDIO_CAT}" name="Audio/Music"/>'
        f'<category id="{BOOKS_CAT}" name="Books/eBooks"/>'
        f'<category id="{ADULT_CAT}" name="XXX/Adult"/>'
        "</categories></caps>"
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


def get_nzb(release_id: str, cache: Optional[Redis]) -> str:
    """Return an NZB document for ``release_id`` using ``cache``.

    If ``cache`` is provided the result is stored under ``nzb:<release_id>``
    with a TTL of 24 hours and retrieved from there on subsequent calls.
    """

    key = f"nzb:{release_id}"
    if cache:
        cached = cache.get(key)
        if cached:
            return cached.decode("utf-8")
        xml = nzb_xml_stub(release_id)
        cache.setex(key, 86400, xml)
        return xml
    return nzb_xml_stub(release_id)
