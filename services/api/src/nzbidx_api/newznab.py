"""Helpers for the Newznab API."""

import os
from typing import Optional

# Optional redis dependency for caching
try:  # pragma: no cover - import guard
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore

ADULT_CATEGORY = 6000


def adult_content_allowed() -> bool:
    """Return ``True`` if XXX content may be shown."""

    allow_xxx = os.getenv("ALLOW_XXX", "false").lower() == "true"
    safesearch_on = os.getenv("SAFESEARCH", "on").lower() != "off"
    return allow_xxx and not safesearch_on


def is_adult_category(cat: Optional[str]) -> bool:
    """Return ``True`` if ``cat`` is an adult category id."""

    try:
        value = int(cat or 0)
    except ValueError:
        return False
    return ADULT_CATEGORY <= value < ADULT_CATEGORY + 1000


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""

    categories = [
        '<category id="2000" name="Movies"/>',
        '<category id="5000" name="TV"/>',
    ]
    if adult_content_allowed():
        categories.append('<category id="6000" name="XXX"/>')
    cats_xml = f"<categories>{''.join(categories)}</categories>"
    return (
        '<caps><server version="0.1" title="nzbidx"/>'
        '<limits max="100" default="50"/>'
        f"{cats_xml}</caps>"
    )


def rss_xml(items: list[dict[str, str]]) -> str:
    """Return a simple RSS feed with the provided items.

    Each ``item`` dict should contain ``title``, ``guid``, ``pubDate``,
    ``category`` and ``link`` keys. No escaping is performed as the values are
    expected to be safe for XML. Adult items are stripped when not allowed.
    """

    allow_adult = adult_content_allowed()
    safe_items = [
        i for i in items if allow_adult or not is_adult_category(i.get("category"))
    ]
    items_xml = "".join(
        "<item>"
        f"<title>{i['title']}</title>"
        f"<guid>{i['guid']}</guid>"
        f"<pubDate>{i['pubDate']}</pubDate>"
        f"<category>{i['category']}</category>"
        f"<link>{i['link']}</link>"
        "</item>"
        for i in safe_items
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


def adult_disabled_xml() -> str:
    """Return an empty RSS feed noting adult content is disabled."""

    return (
        '<rss version="2.0"><channel>'
        "<!-- adult content disabled -->"
        "</channel></rss>"
    )
