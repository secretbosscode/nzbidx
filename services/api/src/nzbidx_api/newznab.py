"""Helpers for the Newznab API."""

import asyncio
import json
import os
import html
import logging
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timezone
from email.utils import format_datetime

from .metrics_log import inc_nzb_cache_hit, inc_nzb_cache_miss

from . import nzb_builder
from .utils import maybe_await

log = logging.getLogger(__name__)

ADULT_CATEGORY_ID = 6000

FAIL_SENTINEL = b"__error__"
FAIL_TTL = 60
SUCCESS_TTL = 86400


class NzbFetchError(Exception):
    """Raised when an NZB document cannot be fetched."""


class NntpConfigError(NzbFetchError):
    """Raised when required NNTP configuration is missing."""


class NntpNoArticlesError(NzbFetchError):
    """Raised when no matching NNTP articles are found."""


class NzbDatabaseError(Exception):
    """Raised when database queries fail while fetching NZB data."""


def is_adult_category(cat: Optional[str]) -> bool:
    """Return ``True`` if ``cat`` is an adult category id."""
    try:
        value = int(cat or 0)
    except ValueError:
        return False
    return ADULT_CATEGORY_ID <= value < ADULT_CATEGORY_ID + 1000


def _default_categories() -> list[dict[str, str]]:
    """Return the full set of built-in Newznab categories.

    ``*_CAT_ID`` environment variables continue to allow overriding the
    primary category IDs.
    """

    return [
        {"id": "0000", "name": "Reserved"},
        {"id": "1000", "name": "Console"},
        {"id": "1010", "name": "Console/NDS"},
        {"id": "1020", "name": "Console/PSP"},
        {"id": "1030", "name": "Console/Wii"},
        {"id": "1040", "name": "Console/XBox"},
        {"id": "1050", "name": "Console/XBox 360"},
        {"id": "1060", "name": "Console/Wiiware"},
        {"id": "1070", "name": "Console/XBox 360 DLC"},
        {"id": os.getenv("MOVIES_CAT_ID", "2000"), "name": "Movies"},
        {"id": "2010", "name": "Movies/Foreign"},
        {"id": "2020", "name": "Movies/Other"},
        {"id": "2030", "name": "Movies/SD"},
        {"id": "2040", "name": "Movies/HD"},
        {"id": "2050", "name": "Movies/BluRay"},
        {"id": "2060", "name": "Movies/3D"},
        {"id": os.getenv("AUDIO_CAT_ID", "3000"), "name": "Audio"},
        {"id": "3010", "name": "Audio/MP3"},
        {"id": "3020", "name": "Audio/Video"},
        {"id": "3030", "name": "Audio/Audiobook"},
        {"id": "3040", "name": "Audio/Lossless"},
        {"id": "4000", "name": "PC"},
        {"id": "4010", "name": "PC/0day"},
        {"id": "4020", "name": "PC/ISO"},
        {"id": "4030", "name": "PC/Mac"},
        {"id": "4040", "name": "PC/Mobile-Other"},
        {"id": "4050", "name": "PC/Games"},
        {"id": "4060", "name": "PC/Mobile-iOS"},
        {"id": "4070", "name": "PC/Mobile-Android"},
        {"id": os.getenv("TV_CAT_ID", "5000"), "name": "TV"},
        {"id": "5020", "name": "TV/Foreign"},
        {"id": "5030", "name": "TV/SD"},
        {"id": "5040", "name": "TV/HD"},
        {"id": "5050", "name": "TV/Other"},
        {"id": "5060", "name": "TV/Sport"},
        {"id": os.getenv("ADULT_CAT_ID", "6000"), "name": "XXX"},
        {"id": "6010", "name": "XXX/DVD"},
        {"id": "6020", "name": "XXX/WMV"},
        {"id": "6030", "name": "XXX/XviD"},
        {"id": "6040", "name": "XXX/x264"},
        {"id": "6045", "name": "XXX/UHD"},
        {"id": "6050", "name": "XXX/Pack"},
        {"id": "6060", "name": "XXX/ImageSet"},
        {"id": "6070", "name": "XXX/Other"},
        {"id": "6080", "name": "XXX/SD"},
        {"id": "6090", "name": "XXX/WEB-DL"},
        {"id": "7000", "name": "Other"},
        {"id": "7010", "name": "Misc"},
        {"id": os.getenv("BOOKS_CAT_ID", "7020"), "name": "EBook"},
        {"id": "7030", "name": "Comics"},
    ]


def _load_categories() -> list[dict[str, str]]:
    """Load categories from config file referenced by ``CATEGORY_CONFIG``.

    The configuration file should contain a JSON array of objects with ``id``
    and ``name`` keys.  When the file cannot be read or is missing, built-in
    defaults are used.
    """

    cfg_path = os.getenv("CATEGORY_CONFIG")
    if cfg_path:
        try:
            data = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
            return [{"id": str(c["id"]), "name": str(c["name"])} for c in data]
        except FileNotFoundError:
            log.warning("category config file not found")
        except Exception:
            log.warning("invalid JSON")
    return _default_categories()


CATEGORIES = _load_categories()
_CATEGORY_MAP = {c["name"]: c["id"] for c in CATEGORIES}
_ID_NAME_MAP = {c["id"]: c["name"] for c in CATEGORIES}
MOVIES_CATEGORY_ID = _CATEGORY_MAP.get("Movies", "2000")
TV_CATEGORY_ID = _CATEGORY_MAP.get("TV", "5000")
AUDIO_CATEGORY_ID = _CATEGORY_MAP.get("Audio", _CATEGORY_MAP.get("Audio/Music", "3000"))
BOOKS_CATEGORY_ID = _CATEGORY_MAP.get("EBook", "7020")


def _collect_category_ids(parent: str) -> list[str]:
    """Return IDs for ``parent`` and any ``parent/*`` subcategories."""

    return [
        c["id"]
        for c in CATEGORIES
        if c["name"] == parent or c["name"].startswith(f"{parent}/")
    ]


def expand_category_ids(ids: list[str]) -> list[str]:
    """Expand parent category IDs to include their subcategories."""

    expanded: list[str] = []
    for cid in ids:
        name = _ID_NAME_MAP.get(cid)
        if not name:
            expanded.append(cid)
            continue
        if "/" in name:
            expanded.append(cid)
        else:
            expanded.extend(_collect_category_ids(name))
    seen: set[str] = set()
    result: list[str] = []
    for c in expanded:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


MOVIE_CATEGORY_IDS = _collect_category_ids("Movies")
TV_CATEGORY_IDS = _collect_category_ids("TV")
AUDIO_CATEGORY_IDS = _collect_category_ids("Audio")
BOOKS_CATEGORY_IDS = _collect_category_ids("EBook")
ADULT_CATEGORY_IDS = _collect_category_ids("XXX")


def _generate_caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    categories = [f'<category id="{c["id"]}" name="{c["name"]}"/>' for c in CATEGORIES]
    cats_xml = f"<categories>{''.join(categories)}</categories>"
    searching_xml = (
        "<searching>"
        '<search available="yes" supportedParams="q,cat,limit,offset"/>'
        "</searching>"
    )
    return (
        '<caps><server version="0.1" title="nzbidx"/>'
        '<limits max="100" default="50"/>'
        f"{searching_xml}{cats_xml}</caps>"
    )


_CACHED_CAPS_XML = _generate_caps_xml()


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    return _CACHED_CAPS_XML


def rss_xml(
    items: list[dict[str, str]],
    *,
    extended: bool = False,
    title: str = "nzbidx",
    link: str = "/",
    description: str = "nzbidx RSS feed",
    language: Optional[str] = None,
    feed_url: Optional[str] = None,
) -> str:
    """Return a simple RSS feed with the provided items.

    Each ``item`` dict should contain ``title``, ``guid``, ``pubDate``,
    ``category`` and ``link`` keys. ``size`` is optional and used for the
    enclosure length when it is present and greater than ``0``. No escaping is
    performed as the values are expected to be safe for XML. ``language`` and
    ``feed_url`` are optional and when ``feed_url`` is provided an ``atom:link``
    element pointing to it is included in the channel.
    """
    channel_date = format_datetime(datetime.now(timezone.utc))
    item_parts = []
    for i in items:
        size = str(i.get("size", ""))
        enclosure = (
            f'<enclosure url="{html.escape(i["link"])}" type="application/x-nzb" length="{html.escape(size)}"/>'
            if size.isdigit() and int(size) > 0
            else ""
        )
        attrs = ""
        if extended:
            attr_parts: list[str] = []
            for key in ("imdbid", "size", "category"):
                val = str(i.get(key, ""))
                if val:
                    attr_parts.append(
                        f'<attr name="{html.escape(key)}" value="{html.escape(val)}"/>'
                    )
            attrs = "".join(attr_parts)
        item_parts.append(
            "".join(
                [
                    "<item>",
                    f"<title>{html.escape(i['title'])}</title>",
                    f"<guid>{html.escape(i['guid'])}</guid>",
                    f"<pubDate>{html.escape(i['pubDate'])}</pubDate>",
                    f"<category>{html.escape(i['category'])}</category>",
                    f"<link>{html.escape(i['link'])}</link>",
                    f"{enclosure}",
                    f"{attrs}",
                    "</item>",
                ]
            )
        )
    items_xml = "".join(item_parts)
    channel_parts = [
        "<channel>",
        f"<title>{html.escape(title)}</title>",
        f"<link>{html.escape(link)}</link>",
        f"<description>{html.escape(description)}</description>",
    ]
    if language:
        channel_parts.append(f"<language>{html.escape(language)}</language>")
    if feed_url:
        channel_parts.append(
            f'<atom:link href="{html.escape(feed_url)}" rel="self" type="application/rss+xml"/>'
        )
    channel_parts.append(f"<pubDate>{html.escape(channel_date)}</pubDate>")
    channel_parts.append(items_xml)
    channel_parts.append("</channel>")

    rss_attrs = ['version="2.0"']
    if feed_url:
        rss_attrs.append('xmlns:atom="http://www.w3.org/2005/Atom"')

    return "".join(
        [
            f"<rss {' '.join(rss_attrs)}>",
            "".join(channel_parts),
            "</rss>",
        ]
    )


async def get_nzb(release_id: str, cache: Optional[Any]) -> str:
    """Return an NZB document for ``release_id`` using an optional in-memory cache.

    The actual NZB building is delegated to
    :func:`nzb_builder.build_nzb_for_release`. When ``cache`` is provided the
    result is stored under ``nzb:<release_id>`` with a TTL of ``SUCCESS_TTL`` and
    retrieved from there on subsequent calls. Failed fetch attempts are cached
    under the same key using ``FAIL_SENTINEL`` for ``FAIL_TTL`` seconds to reduce
    hammering of upstream resources. Any :class:`NzbFetchError` raised by the
    builder is re-raised so callers can handle it explicitly. Any
    :class:`NzbDatabaseError` from the builder is propagated unchanged.
    """

    key = f"nzb:{release_id}"
    if cache:
        try:
            cached = await maybe_await(cache.get(key))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("cache get failed for %s: %s", release_id, exc)
        else:
            if cached:
                inc_nzb_cache_hit()
                if cached == FAIL_SENTINEL:
                    raise NzbFetchError("previous fetch failed")
                if isinstance(cached, (bytes, bytearray)):
                    return cached.decode("utf-8")
                return cached
            inc_nzb_cache_miss()

    try:
        xml = await asyncio.to_thread(nzb_builder.build_nzb_for_release, release_id)
    except NzbDatabaseError:
        raise
    except NzbFetchError:
        if cache:
            try:
                await maybe_await(cache.setex(key, FAIL_TTL, FAIL_SENTINEL))
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("cache setex failed for %s: %s", release_id, exc)
        raise

    if cache:
        try:
            await maybe_await(cache.setex(key, SUCCESS_TTL, xml))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("cache setex failed for %s: %s", release_id, exc)

    return xml
