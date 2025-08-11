"""Entry point for the ingest service."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


try:
    from nzbidx_common.os import OS_RELEASES_ALIAS  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    # ``nzbidx_common`` is an optional sibling package providing shared
    # constants.  The tests run the ingest package in isolation, so fall back
    # to the default alias if the package is not available on ``sys.path``.
    OS_RELEASES_ALIAS = "nzbidx-releases"

from .logging import setup_logging
from .nntp_client import NNTPClient
from .parsers import detect_language, normalize_subject, extract_tags

logger = logging.getLogger(__name__)

# Newznab-style category IDs
CATEGORY_MAP = {
    "reserved": "0000",
    "console": "1000",
    "console_nds": "1010",
    "console_psp": "1020",
    "console_wii": "1030",
    "console_xbox": "1040",
    "console_xbox360": "1050",
    "console_wiiware": "1060",
    "console_xbox360_dlc": "1070",
    "movies": "2000",
    "movies_foreign": "2010",
    "movies_other": "2020",
    "movies_sd": "2030",
    "movies_hd": "2040",
    "movies_bluray": "2050",
    "movies_3d": "2060",
    "audio": "3000",
    "music": "3000",
    "audio_mp3": "3010",
    "audio_video": "3020",
    "audio_audiobook": "3030",
    "audio_lossless": "3040",
    "pc": "4000",
    "pc_0day": "4010",
    "pc_iso": "4020",
    "pc_mac": "4030",
    "pc_mobile_other": "4040",
    "pc_games": "4050",
    "pc_mobile_ios": "4060",
    "pc_mobile_android": "4070",
    "tv": "5000",
    "tv_foreign": "5020",
    "tv_sd": "5030",
    "tv_hd": "5040",
    "tv_other": "5050",
    "tv_sport": "5060",
    "xxx": "6000",
    "xxx_dvd": "6010",
    "xxx_wmv": "6020",
    "xxx_xvid": "6030",
    "xxx_x264": "6040",
    "other": "7000",
    "misc": "7010",
    "ebook": "7020",
    "books": "7020",
    "comics": "7030",
}


def connect_db() -> sqlite3.Connection:
    """Connect to the database and ensure the release table exists."""
    url = os.getenv("DATABASE_URL") or ":memory:"
    conn = sqlite3.connect(url)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS release (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            norm_title TEXT UNIQUE,
            category TEXT,
            language TEXT,
            tags TEXT
        )
        """
    )
    return conn


def connect_opensearch() -> Optional[object]:
    """Return an OpenSearch client if available, else None."""
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        from opensearchpy import OpenSearch  # type: ignore

        return OpenSearch(url, timeout=2)
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.info("opensearch_unavailable", extra={"error": str(exc)})
        return None


def insert_release(
    conn: sqlite3.Connection,
    norm_title: str,
    category: Optional[str],
    language: Optional[str],
    tags: list[str],
) -> bool:
    """Insert a release into the database if new. Returns True if inserted."""
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO release (norm_title, category, language, tags) VALUES (?, ?, ?, ?)",
        (norm_title, category, language, ",".join(tags) if tags else None),
    )
    conn.commit()
    return cur.rowcount > 0


_os_warned = False


def index_release(
    client: Optional[object],
    norm_title: str,
    *,
    category: Optional[str] = None,
    language: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> None:
    """Index the release into OpenSearch (no-op if client is None)."""
    global _os_warned
    if not client:
        return
    body: dict[str, object] = {"norm_title": norm_title}
    if category:
        body["category"] = category
    if language:
        body["language"] = language
    if tags:
        body["tags"] = tags
    try:  # pragma: no cover - network errors
        client.index(
            index=OS_RELEASES_ALIAS,
            id=norm_title,
            body=body,
            refresh=False,
        )
    except Exception as exc:  # pragma: no cover - network errors
        if not _os_warned:
            logger.warning("opensearch_index_failed", extra={"error": str(exc)})
            _os_warned = True


def _infer_category(subject: str) -> Optional[str]:
    """Heuristic category detection from the raw subject."""
    s = subject.lower()

    # Prefer explicit bracketed tags like "[music]" or "[books]" if present.
    for tag in extract_tags(subject):
        if tag in CATEGORY_MAP:
            return CATEGORY_MAP[tag]

    # Fallback explicit markers (redundant, but resilient if extract_tags changes)
    if "[movies]" in s or "[movie]" in s:
        return CATEGORY_MAP["movies"]
    if "[tv]" in s:
        return CATEGORY_MAP["tv"]
    if "[music]" in s or "[audio]" in s:
        return CATEGORY_MAP["audio"]
    if "[books]" in s or "[book]" in s or "[ebook]" in s:
        return CATEGORY_MAP["ebook"]
    if "[xxx]" in s:
        return CATEGORY_MAP["xxx"]
    if any(
        k in s
        for k in ("brazzers", "realitykings", "onlyfans", "pornhub", "adult", "xxx")
    ):
        if "dvd" in s:
            return CATEGORY_MAP["xxx_dvd"]
        if "wmv" in s:
            return CATEGORY_MAP["xxx_wmv"]
        if "xvid" in s:
            return CATEGORY_MAP["xxx_xvid"]
        if "x264" in s or "h264" in s:
            return CATEGORY_MAP["xxx_x264"]
        return CATEGORY_MAP["xxx"]

    # TV
    if re.search(r"s\d{1,2}e\d{1,2}", s) or "season" in s or "episode" in s:
        if "sport" in s or "sports" in s:
            return CATEGORY_MAP["tv_sport"]
        if any(k in s for k in ("1080p", "720p", "x264", "x265", "hd")):
            return CATEGORY_MAP["tv_hd"]
        if any(k in s for k in ("xvid", "dvdrip", "sd")):
            return CATEGORY_MAP["tv_sd"]
        return CATEGORY_MAP["tv"]

    # Movies
    if any(k in s for k in ("bluray", "blu-ray")):
        return CATEGORY_MAP["movies_bluray"]
    if "3d" in s:
        return CATEGORY_MAP["movies_3d"]
    if any(k in s for k in ("1080p", "720p", "x264", "x265", "hdrip", "webrip", "hd")):
        return CATEGORY_MAP["movies_hd"]
    if any(k in s for k in ("dvdrip", "xvid", "cam", "ts", "sd")):
        return CATEGORY_MAP["movies_sd"]

    # Audio
    if "audiobook" in s or "audio book" in s:
        return CATEGORY_MAP["audio_audiobook"]
    if any(k in s for k in ("flac", "lossless")):
        return CATEGORY_MAP["audio_lossless"]
    if any(k in s for k in ("mp3", "aac", "m4a")):
        return CATEGORY_MAP["audio_mp3"]
    if "video" in s and "music" in s:
        return CATEGORY_MAP["audio_video"]
    if any(k in s for k in ("album", "single", "music")):
        return CATEGORY_MAP["audio"]

    # Books
    if any(k in s for k in ("cbz", "cbr", "comic")):
        return CATEGORY_MAP["comics"]
    if any(k in s for k in ("epub", "mobi", "pdf", "ebook", "isbn")):
        return CATEGORY_MAP["ebook"]

    return None


def main() -> int:
    """Run the ingest service."""
    load_dotenv()
    setup_logging()

    # Connect to NNTP (dry-run safe)
    client = NNTPClient()
    client.connect()

    db = connect_db()
    os_client = connect_opensearch()

    # Simulated subjects batch (idempotent insert/OS index)
    subjects = [
        "Test Release One [music]",
        "Another Release [books]",
        "Test Release One [music]",  # duplicate on purpose
    ]

    for subject in subjects:
        # Normalized title and extracted tags (from parsers)
        norm_title, tags = normalize_subject(subject, with_tags=True)
        norm_title = norm_title.lower()

        # Language & category heuristics
        language = detect_language(subject) or "en"
        category = _infer_category(subject)

        if insert_release(db, norm_title, category, language, tags):
            index_release(
                os_client,
                norm_title,
                category=category,
                language=language,
                tags=tags,
            )

    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
