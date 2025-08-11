"""Parsers for ingest service."""

from __future__ import annotations

import re
from typing import Dict, List, Optional

# Language detection tokens found in many Usenet subjects
LANGUAGE_TOKENS: Dict[str, str] = {
    "[ITA]": "it",
    "[FRENCH]": "fr",
    "[GERMAN]": "de",
}

_TAG_RE = re.compile(r"\[([^\[\]]+)\]")


def extract_tags(subject: str) -> list[str]:
    """Extract lowercased tags from bracketed segments in ``subject``."""
    if not subject:
        return []
    tags: list[str] = []
    for match in _TAG_RE.finditer(subject):
        content = match.group(1)
        for tag in re.split(r"[\s,]+", content):
            tag = tag.strip().lower()
            if tag:
                tags.append(tag)
    return tags


def detect_language(subject: str) -> Optional[str]:
    """Return a language code if a known token is present in the subject."""
    if not subject:
        return None
    upper = subject.upper()
    for token, code in LANGUAGE_TOKENS.items():
        if token in upper:
            return code
    return None


def extract_music_tags(subject: str) -> Dict[str, str]:
    """Return music related tags from a subject line.

    Handles scene style strings like ``Artist-Album-2021-FLAC`` or
    ``Artist-Album-2021-MP3-320``. The returned dict may contain the keys
    ``artist``, ``album``, ``year``, ``format`` and ``bitrate``.
    """
    pattern = re.compile(
        r"(?P<artist>[^-]+)-(?P<album>[^-]+)-(?P<year>\d{4})-"
        r"(?P<format>FLAC|MP3)(?:-(?P<bitrate>\d{3}))?",
        re.IGNORECASE,
    )
    match = pattern.search(subject)
    if not match:
        return {}

    tags = match.groupdict()
    tags = {k: v.replace(".", " ") if v else v for k, v in tags.items() if v}
    if "format" in tags:
        tags["format"] = tags["format"].upper()
    return tags


def extract_book_tags(subject: str) -> Dict[str, str]:
    """Return book related tags from a subject line.

    Expected patterns look like ``Author-Title-2020-EPUB`` or optionally
    include an ISBN number: ``Author-Title-2020-PDF-1234567890``.
    Returns a dict with ``author``, ``title``, ``year``, ``format`` and
    optional ``isbn`` keys when found.
    """
    pattern = re.compile(
        r"(?P<author>[^-]+)-(?P<title>[^-]+)-(?P<year>\d{4})-"
        r"(?P<format>EPUB|MOBI|PDF)(?:-(?P<isbn>\d{10,13}))?",
        re.IGNORECASE,
    )
    match = pattern.search(subject)
    if not match:
        return {}

    tags = match.groupdict()
    tags = {k: v.replace(".", " ") if v else v for k, v in tags.items() if v}
    tags["format"] = tags["format"].upper()
    return tags


def extract_xxx_tags(subject: str) -> Dict[str, str]:
    """Return adult content related tags from a subject line.

    Supports two common patterns:

    ``Studio.Name.2022.1080p`` -> ``studio``, ``date`` and ``resolution``
    ``Site.Name.2023.07.12``   -> ``site`` and ``date``
    """
    studio_re = re.compile(
        r"(?P<studio>[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+)\.(?P<date>\d{4})\.(?P<resolution>\d{3,4}p)",
        re.IGNORECASE,
    )
    site_re = re.compile(
        r"(?P<site>[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+)\.(?P<date>\d{4}\.\d{2}\.\d{2})",
        re.IGNORECASE,
    )

    match = studio_re.search(subject)
    if match:
        tags = match.groupdict()
        tags["studio"] = tags["studio"].replace(".", " ")
        return {k: v for k, v in tags.items() if v}

    match = site_re.search(subject)
    if match:
        tags = match.groupdict()
        tags["site"] = tags["site"].replace(".", " ")
        return {k: v for k, v in tags.items() if v}

    return {}


def normalize_subject(
    subject: str, *, with_tags: bool = False
) -> tuple[str, List[str]] | str:
    """Return a cleaned, human-readable version of a Usenet subject line.

    Lightweight normalization:
    - Convert separators ('.', '_') to spaces
    - Remove explicit 'yEnc' markers
    - Drop part counters like '(01/15)' or '[12345/12346]'
    - Remove language tokens (e.g., '[FRENCH]', '[GERMAN]', '[ITA]')
    - Remove common filler words (e.g., 'repost', 'sample')
    - Collapse whitespace and trim separators

    Also extracts hints via extract_* helpers and returns them as lowercase tags
    (when ``with_tags=True``).
    """
    if not subject:
        return ("", []) if with_tags else ""

    # Extract bracketed tags and structured hints before cleaning. The specific
    # ``extract_*`` helpers operate on the raw subject, so run them before we
    # strip punctuation.
    generic_tags = extract_tags(subject)
    tag_dict: Dict[str, str] = {}
    for extractor in (extract_music_tags, extract_book_tags, extract_xxx_tags):
        tag_dict.update(extractor(subject))
    for t in extract_tags(subject):
        tag_dict[t] = t

    # Convert common separators to spaces.
    cleaned = subject.replace(".", " ").replace("_", " ")

    # Remove bracketed tags.
    cleaned = _TAG_RE.sub("", cleaned)

    # Remove explicit yEnc markers.
    cleaned = re.sub(r"(?i)\byenc\b", "", cleaned)

    # Drop part/size information such as "(01/15)" or "[12345/12346]".
    cleaned = re.sub(r"[\(\[]\s*\d+\s*/\s*\d+\s*[\)\]]", "", cleaned)

    # Remove language tokens based on LANGUAGE_TOKENS keys.
    if LANGUAGE_TOKENS:
        tokens_pattern = "|".join(map(re.escape, LANGUAGE_TOKENS.keys()))
        cleaned = re.sub(tokens_pattern, "", cleaned, flags=re.IGNORECASE)

    # Remove common filler words.
    fillers = ("repost", "sample")
    cleaned = re.sub(
        rf"\b({'|'.join(map(re.escape, fillers))})\b", "", cleaned, flags=re.IGNORECASE
    )

    # Collapse whitespace and trim leading/trailing separators or dashes.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"^[-\s]+|[-\s]+$", "", cleaned)

    tags = sorted(
        {
            *generic_tags,
            *[value.lower() for value in tag_dict.values() if value],
        }
    )
    if with_tags:
        return cleaned, tags
    return cleaned


def parse(
    headers: List[Dict[str, object]],
    *,
    db: Optional[object] = None,
    os_client: Optional[object] = None,
) -> None:
    """Parse NNTP ``headers`` and index the results into OpenSearch.

    Parameters
    ----------
    headers:
        Iterable of article headers as returned by :meth:`NNTPClient.xover`.
    db, os_client:
        Optional database and OpenSearch client instances.  When omitted the
        function will create its own connections via ``connect_db`` and
        ``connect_opensearch`` from :mod:`nzbidx_ingest.main`.
    """

    from email.utils import parsedate_to_datetime

    # Lazy imports to avoid expensive setup when the helper is used in isolation
    from .main import (
        connect_db,
        connect_opensearch,
        insert_release,
        index_release,
        _infer_category,
    )

    if db is None:
        db = connect_db()
    if os_client is None:
        os_client = connect_opensearch()

    for header in headers:
        subject = str(header.get("subject", ""))

        # Normalised title and extracted tags
        norm_title, tags = normalize_subject(subject, with_tags=True)
        norm_title = norm_title.lower()

        # Build a dedupe key incorporating the posting day when available
        posted = header.get("date")
        day_bucket = ""
        if posted:
            try:
                day_bucket = parsedate_to_datetime(str(posted)).strftime("%Y-%m-%d")
            except Exception:
                day_bucket = ""
        dedupe_key = f"{norm_title}:{day_bucket}" if day_bucket else norm_title

        language = detect_language(subject)
        category = _infer_category(subject)

        inserted = insert_release(db, dedupe_key, category, language, tags)
        if inserted:
            index_release(
                os_client,
                dedupe_key,
                category=category,
                language=language,
                tags=tags,
            )
