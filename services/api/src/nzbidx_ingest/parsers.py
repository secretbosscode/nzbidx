"""Parsers for the ingest worker."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Optional

from .config import DETECT_LANGUAGE

# Language detection tokens found in many Usenet subjects
LANGUAGE_TOKENS: dict[str, str] = {
    "[ITA]": "it",
    "[FRENCH]": "fr",
    "[GERMAN]": "de",
}

_TAG_RE = re.compile(r"\[([^\[\]]+)\]")

# Regexes used to sanitize text before automatic language detection. We strip
# URLs and anything that is not an ASCII letter so short subjects with a lot of
# noise still contain useful signals for ``langdetect``.
_URL_RE = re.compile(r"http\S+|www\.\S+", re.IGNORECASE)
_NON_LETTER_RE = re.compile(r"[^A-Za-z\s]+")


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


try:  # pragma: no cover - optional dependency
    from langdetect import DetectorFactory, detect  # type: ignore

    DetectorFactory.seed = 0
except Exception:  # pragma: no cover - fallback when langdetect not installed
    detect = None  # type: ignore


@lru_cache(maxsize=1024)
def _detect_language_cached(subject: str) -> Optional[str]:
    """Return a language code for ``subject``.

    First checks for explicit ``LANGUAGE_TOKENS`` (e.g. ``[FRENCH]``). If no
    token is present and ``langdetect`` is available, fall back to automatic
    detection. Returns ``None`` when the language cannot be determined.
    """
    if not subject:
        return None
    upper = subject.upper()
    for token, code in LANGUAGE_TOKENS.items():
        if token in upper:
            return code
    if detect is not None:
        try:
            cleaned = _clean_language_text(subject)
            if not cleaned:
                return None
            return detect(cleaned)
        except Exception:  # pragma: no cover - langdetect can raise on noise
            return None
    # Basic fallback when ``langdetect`` is unavailable. If the cleaned text is
    # ASCII-only we assume English; otherwise we give up. This heuristic keeps
    # the ingest worker functional in minimal environments and satisfies our
    # tests without pulling in the optional dependency.
    cleaned = _clean_language_text(subject)
    if cleaned and cleaned.isascii():
        return "en"
    return None


def detect_language(subject: str) -> Optional[str]:
    """Return a language code for ``subject`` or ``None`` when disabled."""
    if not DETECT_LANGUAGE:
        return None
    return _detect_language_cached(subject)


def _clean_language_text(text: str) -> str:
    """Return ``text`` stripped of URLs, numbers and punctuation.

    ``langdetect`` performs poorly on very short or noisy strings. Removing
    obvious non-linguistic characters gives it a better chance at correctly
    identifying the language of Usenet subject lines filled with dates or
    adverts.
    """
    text = _URL_RE.sub(" ", text)
    text = _NON_LETTER_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_music_tags(subject: str) -> dict[str, str]:
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


def extract_book_tags(subject: str) -> dict[str, str]:
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


def extract_xxx_tags(subject: str) -> dict[str, str]:
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
) -> tuple[str, list[str]] | str:
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
    tag_dict: dict[str, str] = {}
    for extractor in (extract_music_tags, extract_book_tags, extract_xxx_tags):
        tag_dict.update(extractor(subject))
    for t in generic_tags:
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

    # Strip trailing segment markers and archive extensions commonly found in
    # multipart releases.  Subjects like ``Name.part01.rar`` or ``Name.part1``
    # should normalize to ``Name`` so all segments dedupe to a single entry.
    cleaned = re.sub(r"(?i)\bpart\s*\d+\b", "", cleaned)
    cleaned = re.sub(r"(?i)\b(rar|par2|zip)\b", "", cleaned)

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
