"""Entry point for the ingest worker."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional, Any
from urllib.parse import urlparse, urlunparse

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
from .parsers import extract_tags

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


# Keyword hints used to map NNTP groups to categories
GROUP_CATEGORY_HINTS: list[tuple[str, str]] = [
    ("xbox360", "console_xbox360"),
    ("xbox", "console_xbox"),
    ("wiiware", "console_wiiware"),
    ("wii", "console_wii"),
    ("psp", "console_psp"),
    ("playstation", "console_psp"),
    ("nds", "console_nds"),
    ("console", "console"),
    ("0day", "pc_0day"),
    ("iso", "pc_iso"),
    ("mac", "pc_mac"),
    ("ios", "pc_mobile_ios"),
    ("android", "pc_mobile_android"),
    ("games", "pc_games"),
    ("pc", "pc"),
    ("movies", "movies"),
    ("movie", "movies"),
    ("video", "movies"),
    ("tv", "tv"),
    ("series", "tv"),
    ("sport", "tv_sport"),
    ("music", "audio"),
    ("audio", "audio"),
    ("mp3", "audio_mp3"),
    ("flac", "audio_lossless"),
    ("audiobook", "audio_audiobook"),
    ("ebooks", "ebook"),
    ("ebook", "ebook"),
    ("book", "ebook"),
    ("books", "ebook"),
    ("xxx", "xxx"),
    ("sex", "xxx"),
    ("adult", "xxx"),
    ("comics", "comics"),
    ("comic", "comics"),
    ("misc", "misc"),
    ("other", "other"),
]


try:  # pragma: no cover - optional dependency
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - optional dependency
    create_engine = None  # type: ignore
    text = None  # type: ignore


def connect_db() -> Any:
    """Connect to the database and ensure the release table exists.

    If ``DATABASE_URL`` points at PostgreSQL the connection will use the
    ``psycopg`` driver.  When that driver is missing or unavailable, the
    function falls back to an in-memory SQLite database so the ingest worker
    can still run in a degraded mode.
    """

    url = os.getenv("DATABASE_URL")
    if not url:
        logger.warning("database_url_missing")
        url = ":memory:"
        logger.warning("sqlite_fallback", extra={"url": url})
    parsed = urlparse(url)

    if parsed.scheme.startswith("postgres"):
        if not parsed.netloc and parsed.path:
            url = f"{parsed.scheme}://{parsed.path.lstrip('/')}"
            parsed = urlparse(url)
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        if not create_engine or not text:
            logger.warning("sqlalchemy_unavailable")
            raise RuntimeError("sqlalchemy is required for PostgreSQL URLs")
        parsed = urlparse(url)

        def _connect(u: str) -> Any:
            engine = create_engine(u, echo=False, future=True)
            with engine.connect() as conn:  # type: ignore[call-arg]
                for stmt in (
                    "CREATE EXTENSION IF NOT EXISTS vector",
                    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
                    (
                        """
                        CREATE TABLE IF NOT EXISTS release (
                            id SERIAL PRIMARY KEY,
                            norm_title TEXT UNIQUE,
                            category TEXT,
                            language TEXT,
                            tags TEXT
                        )
                        """
                    ),
                ):
                    try:
                        conn.execute(text(stmt))
                        conn.commit()
                    except Exception as exc:
                        # Creating extensions requires superuser privileges.  If
                        # unavailable, log the failure and roll back so that
                        # subsequent statements can proceed.
                        conn.rollback()
                        if stmt.lstrip().upper().startswith("CREATE EXTENSION"):
                            logger.warning(
                                "extension_unavailable",
                                extra={"stmt": stmt, "error": str(exc)},
                            )
                        else:
                            raise
            return engine.raw_connection()

        try:
            return _connect(url)
        except ModuleNotFoundError as exc:  # pragma: no cover - missing driver
            logger.warning("psycopg_unavailable", extra={"error": str(exc)})
            logger.warning("sqlite_fallback", extra={"url": ":memory:"})
            return sqlite3.connect(":memory:")
        except Exception as exc:  # pragma: no cover - network errors
            msg = str(getattr(exc, "orig", exc)).lower()
            if "does not exist" not in msg and "invalid catalog name" not in msg:
                raise
            dbname = parsed.path.lstrip("/")
            admin_url = urlunparse(parsed._replace(path="/postgres"))
            engine = create_engine(admin_url, echo=False, future=True)
            with engine.begin() as conn:  # type: ignore[call-arg]
                conn.execute(text(f'CREATE DATABASE "{dbname}"'))
            engine.dispose()
            return _connect(url)

    # Treat remaining URLs as SQLite database files.  Only attempt to create
    # directories for plain file paths; URLs with a scheme (``foo://``) should
    # be handled by their respective drivers instead.
    if url != ":memory:" and "://" not in url:
        path = Path(url)
        path.parent.mkdir(parents=True, exist_ok=True)
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
    conn: Any,
    norm_title: str,
    category: Optional[str],
    language: Optional[str],
    tags: list[str],
) -> bool:
    """Insert a release into the database if new. Returns True if inserted."""
    cur = conn.cursor()
    params = (norm_title, category, language, ",".join(tags) if tags else None)
    if conn.__class__.__module__.startswith("sqlite3"):
        cur.execute(
            "INSERT OR IGNORE INTO release (norm_title, category, language, tags) VALUES (?, ?, ?, ?)",
            params,
        )
    else:
        cur.execute(
            "INSERT INTO release (norm_title, category, language, tags) VALUES (%s, %s, %s, %s) ON CONFLICT (norm_title) DO NOTHING",
            params,
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


def _infer_category(subject: str, group: Optional[str] = None) -> Optional[str]:
    """Heuristic category detection from the raw subject or group."""
    s = subject.lower()

    if group:
        g = group.lower()
        for key, cat in GROUP_CATEGORY_HINTS:
            if key in g:
                return CATEGORY_MAP[cat]

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
    """Run the ingest worker."""
    load_dotenv()
    setup_logging()
    from .ingest_loop import run_forever

    run_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
