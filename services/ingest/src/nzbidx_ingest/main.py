"""Entry point for the ingest service."""

from __future__ import annotations

from dotenv import load_dotenv

from .logging import setup_logging
from .nntp_client import NNTPClient
from .parsers import extract_tags, normalize_subject

import logging
import os
import sqlite3
from typing import Optional


logger = logging.getLogger(__name__)

CATEGORY_MAP = {"music": "3000", "books": "7000", "xxx": "6000"}


def connect_db() -> sqlite3.Connection:
    """Connect to the database and ensure the release table exists."""

    url = os.getenv("DATABASE_URL") or ":memory:"
    conn = sqlite3.connect(url)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS release (id INTEGER PRIMARY KEY AUTOINCREMENT, norm_title TEXT UNIQUE, category TEXT, language TEXT, tags TEXT)"
    )
    return conn


def connect_opensearch() -> Optional[object]:
    """Return an OpenSearch client if possible."""

    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        from opensearchpy import OpenSearch

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
    """Insert a release into the database if new."""

    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO release (norm_title, category, language, tags) VALUES (?, ?, ?, ?)",
        (norm_title, category, language, ",".join(tags) if tags else None),
    )
    conn.commit()
    return cur.rowcount > 0


def index_release(
    client: Optional[object],
    norm_title: str,
    category: Optional[str],
    language: Optional[str],
    tags: list[str],
) -> None:
    """Index the release into OpenSearch."""

    if not client:
        return
    body = {"norm_title": norm_title}
    if category:
        body["category"] = category
    if language:
        body["language"] = language
    if tags:
        body["tags"] = tags
    try:  # pragma: no cover - network errors
        client.index(
            index="nzbidx-releases-v1", id=norm_title, body=body, refresh=True
        )
    except Exception as exc:  # pragma: no cover - network errors
        logger.warning("opensearch_index_failed", extra={"error": str(exc)})


def main() -> int:
    """Run the ingest service."""
    load_dotenv()
    setup_logging()
    client = NNTPClient()
    client.connect()

    db = connect_db()
    os_client = connect_opensearch()

    subjects = [
        "Test Release One [music]",
        "Another Release [books]",
        "Test Release One [music]",
    ]
    for subject in subjects:
        norm_title = normalize_subject(subject).lower()
        tags = extract_tags(subject)
        category = next((CATEGORY_MAP[t] for t in tags if t in CATEGORY_MAP), None)
        language = "en"
        if insert_release(db, norm_title, category, language, tags):
            index_release(os_client, norm_title, category, language, tags)

    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
