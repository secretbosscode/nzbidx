"""Helpers for the Newznab API."""

import os
from typing import Optional

from .middleware_circuit import CircuitOpenError, call_with_retry, redis_breaker
from .otel import start_span

# Optional redis dependency for caching
try:  # pragma: no cover - import guard
    from redis import Redis
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore

from . import nzb_builder

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


# Customizable category IDs via env vars
MOVIES_CAT = os.getenv("MOVIES_CAT_ID", "2000")
TV_CAT = os.getenv("TV_CAT_ID", "5000")
AUDIO_CAT = os.getenv("AUDIO_CAT_ID", "3000")
BOOKS_CAT = os.getenv("BOOKS_CAT_ID", "7000")
ADULT_CAT = os.getenv("ADULT_CAT_ID", "6000")


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    movies = os.getenv("MOVIES_CAT_ID", MOVIES_CAT)
    tv = os.getenv("TV_CAT_ID", TV_CAT)
    audio = os.getenv("AUDIO_CAT_ID", AUDIO_CAT)
    books = os.getenv("BOOKS_CAT_ID", BOOKS_CAT)
    adult = os.getenv("ADULT_CAT_ID", ADULT_CAT)
    categories = [
        f'<category id="{movies}" name="Movies"/>',
        f'<category id="{tv}" name="TV"/>',
        f'<category id="{audio}" name="Audio/Music"/>',
        f'<category id="{books}" name="Books/eBooks"/>',
    ]
    if adult_content_allowed():
        categories.append(f'<category id="{adult}" name="XXX/Adult"/>')
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

    The actual NZB building is delegated to :func:`nzb_builder.build_nzb_for_release`
    which currently returns a stub XML document.  When ``cache`` is provided the
    result is stored under ``nzb:<release_id>`` with a TTL of 24 hours and
    retrieved from there on subsequent calls.
    """
    key = f"nzb:{release_id}"
    if cache:
        try:
            with start_span("redis.get"):
                cached = call_with_retry(redis_breaker, "redis", cache.get, key)
            if cached:
                return cached.decode("utf-8")
            xml = nzb_builder.build_nzb_for_release(release_id)
            with start_span("redis.setex"):
                call_with_retry(redis_breaker, "redis", cache.setex, key, 86400, xml)
            return xml
        except CircuitOpenError:
            raise
        except Exception:
            pass
    return nzb_builder.build_nzb_for_release(release_id)


def adult_disabled_xml() -> str:
    """Return an empty RSS feed noting adult content is disabled."""
    return (
        '<rss version="2.0"><channel>'
        "<!-- adult content disabled -->"
        "</channel></rss>"
    )
