"""Parsers for the ingest worker."""

from __future__ import annotations

import logging
import re
import sys
from functools import lru_cache
from typing import Optional

from .config import AUDIO_EXTENSIONS, BOOK_EXTENSIONS, DETECT_LANGUAGE

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

# Precompiled regular expressions for tag extraction and subject normalization.
_AUDIO_FORMATS = "|".join(AUDIO_EXTENSIONS)
_BOOK_FORMATS = "|".join(BOOK_EXTENSIONS)

MUSIC_TAG_RE = re.compile(
    rf"(?P<artist>[^-]+)-(?P<album>[^-]+)-(?P<year>\d{{4}})-"
    rf"(?P<format>{_AUDIO_FORMATS})(?:-(?P<bitrate>\d{{3}}))?",
    re.IGNORECASE,
)

BOOK_TAG_RE = re.compile(
    rf"(?P<author>[^-]+)-(?P<title>[^-]+)-(?P<year>\d{{4}})-"
    rf"(?P<format>{_BOOK_FORMATS})(?:-(?P<isbn>\d{{10,13}}))?",
    re.IGNORECASE,
)

XXX_STUDIO_RE = re.compile(
    r"(?P<studio>[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+)\.(?P<date>\d{4})\.(?P<resolution>\d{3,4}p)",
    re.IGNORECASE,
)

XXX_SITE_RE = re.compile(
    r"(?P<site>[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+)\.(?P<date>\d{4}\.\d{2}\.\d{2})",
    re.IGNORECASE,
)

YENC_RE = re.compile(r"\byenc\b", re.IGNORECASE)
PART_SIZE_RE = re.compile(r"[\(\[]\s*\d+\s*/\s*\d+\s*[\)\]]")
FILLER_RE = re.compile(r"\b(?:repost|sample)\b", re.IGNORECASE)
PART_RE = re.compile(r"\bpart\s*\d+\b", re.IGNORECASE)
ARCHIVE_RE = re.compile(r"\b(rar|par2|zip)\b", re.IGNORECASE)
_FILE_EXT_RE = re.compile(r"\.([A-Za-z0-9]{2,4})\b")

logger = logging.getLogger(__name__)

_PYTHON_UNSUPPORTED_FOR_LANGDETECT = sys.version_info >= (3, 13)


def extract_tags(subject: str) -> list[str]:
    """Extract lowercased tags from bracketed segments in ``subject``."""
    if not subject:
        return []
    if "[" not in subject or "]" not in subject:
        return []
    tags: list[str] = []
    for match in _TAG_RE.finditer(subject):
        content = match.group(1)
        for tag in re.split(r"[\s,]+", content):
            tag = tag.strip().lower()
            if tag:
                tags.append(tag)
    return tags


def extract_file_extension(subject: str) -> str | None:
    """Return the lowercased file extension from ``subject`` if present."""
    match = _FILE_EXT_RE.search(subject)
    if match:
        return match.group(1).lower()
    return None


_SEGMENT_RE = re.compile(r"\((\d+)/\d+\)")


def extract_segment_number(subject: str) -> int:
    """Return the segment number parsed from ``subject`` if possible."""
    match = _SEGMENT_RE.search(subject)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 1
    return 1


if _PYTHON_UNSUPPORTED_FOR_LANGDETECT:
    detect = None  # type: ignore
    logger.warning(
        "langdetect disabled: Python %s.%s is not supported",
        sys.version_info.major,
        sys.version_info.minor,
        extra={"event": "langdetect_python_guard"},
    )
else:
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
    return " ".join(text.split())


def extract_music_tags(subject: str) -> dict[str, str]:
    """Return music related tags from a subject line.

    Handles scene style strings like ``Artist-Album-2021-FLAC`` or
    ``Artist-Album-2021-MP3-320``. The returned dict may contain the keys
    ``artist``, ``album``, ``year``, ``format`` and ``bitrate``.
    """
    match = MUSIC_TAG_RE.search(subject)
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
    match = BOOK_TAG_RE.search(subject)
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
    match = XXX_STUDIO_RE.search(subject)
    if match:
        tags = match.groupdict()
        tags["studio"] = tags["studio"].replace(".", " ")
        return {k: v for k, v in tags.items() if v}

    match = XXX_SITE_RE.search(subject)
    if match:
        tags = match.groupdict()
        tags["site"] = tags["site"].replace(".", " ")
        return {k: v for k, v in tags.items() if v}

    return {}


@lru_cache(maxsize=8192)
def _normalize_cached(subject: str, lowercase: bool) -> str:
    """Return the normalized ``subject`` string."""
    cleaned = subject.replace(".", " ").replace("_", " ")
    cleaned = _TAG_RE.sub("", cleaned)
    cleaned = YENC_RE.sub("", cleaned)
    cleaned = PART_SIZE_RE.sub("", cleaned)
    if LANGUAGE_TOKENS:
        tokens_pattern = "|".join(map(re.escape, LANGUAGE_TOKENS.keys()))
        cleaned = re.sub(tokens_pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = FILLER_RE.sub("", cleaned)
    cleaned = PART_RE.sub("", cleaned)
    cleaned = ARCHIVE_RE.sub("", cleaned)
    cleaned = " ".join(cleaned.split())
    cleaned = cleaned.strip("- ")
    if lowercase:
        cleaned = cleaned.lower()
    return cleaned


def normalize_subject(
    subject: str, *, with_tags: bool = False, lowercase: bool = True
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
    (when ``with_tags=True``). By default the cleaned title is lowercased; pass
    ``lowercase=False`` to preserve the original casing.
    """
    if not subject:
        return ("", []) if with_tags else ""

    generic_tags = extract_tags(subject)
    lower_subject = subject.lower()
    tag_dict: dict[str, str] = {}

    if any(ext.lower() in lower_subject for ext in AUDIO_EXTENSIONS):
        tag_dict.update(extract_music_tags(subject))

    if any(ext.lower() in lower_subject for ext in BOOK_EXTENSIONS):
        tag_dict.update(extract_book_tags(subject))

    if any(
        t in lower_subject for t in ("1080p", "720p", "2160p", "480p", ".20", ".19")
    ):
        tag_dict.update(extract_xxx_tags(subject))

    cleaned = _normalize_cached(subject, lowercase)

    tags = list(
        dict.fromkeys(
            [
                *generic_tags,
                *[value.lower() for value in tag_dict.values() if value],
            ]
        )
    )

    if with_tags:
        return cleaned, tags
    return cleaned
