"""Entry point for the ingest service."""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

from dotenv import load_dotenv

from .logging import setup_logging
from .nntp_client import NNTPClient
from .parsers import detect_language, normalize_subject, extract_tags

logger = logging.getLogger(__name__)

# Newznab-style category IDs
CATEGORY_MAP = {"music": "3000", "books": "7000", "xxx": "6000"}


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
            index="nzbidx-releases-v1",
            id=norm_title,
            body=body,
            refresh=True,
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
    if "[music]" in s:
        return CATEGORY_MAP["music"]
    if "[books]" in s or "[book]" in s:
        return CATEGORY_MAP["books"]
    if "[xxx]" in s:
        return CATEGORY_MAP["xxx"]

    # Heuristic keyword checks
    if any(k in s for k in ("flac", "mp3", "aac", "album")):
        return CATEGORY_MAP["music"]
    if any(k in s for k in ("epub", "mobi", "pdf", "ebook", "isbn")):
        return CATEGORY_MAP["books"]
    if any(
        k in s
        for k in ("brazzers", "realitykings", "onlyfans", "pornhub", "adult", "xxx")
    ):
        return CATEGORY_MAP["xxx"]
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
