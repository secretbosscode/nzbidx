"""Entry point for the ingest worker."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional, Any, Iterable
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

from .config import (
    INGEST_OS_BULK,
    opensearch_timeout_seconds,
)
from .logging import setup_logging
from .parsers import extract_tags
from .resource_monitor import install_signal_handlers, start_memory_logger

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
    "xxx_uhd": "6045",
    "xxx_pack": "6050",
    "xxx_imageset": "6060",
    "xxx_other": "6070",
    "xxx_sd": "6080",
    "xxx_webdl": "6090",
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

    ``DATABASE_URL`` must point to a persistent database.  If the URL uses a
    PostgreSQL scheme the ``psycopg`` driver and ``sqlalchemy`` are required.
    Missing configuration or driver dependencies raises :class:`RuntimeError`.
    """

    url = os.getenv("DATABASE_URL")
    if not url:
        logger.error("database_url_missing")
        raise RuntimeError("DATABASE_URL environment variable is required")
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
                    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
                    (
                        """
                        CREATE TABLE IF NOT EXISTS release (
                            id BIGSERIAL PRIMARY KEY,
                            norm_title TEXT UNIQUE,
                            category TEXT,
                            language TEXT NOT NULL DEFAULT 'und',
                            tags TEXT NOT NULL DEFAULT '',
                            source_group TEXT,
                            size_bytes BIGINT,
                            posted_at TIMESTAMPTZ,
                            segments JSONB,
                            has_parts BOOLEAN NOT NULL DEFAULT FALSE,
                            part_count INT NOT NULL DEFAULT 0
                        )
                        """
                    ),
                    "DROP INDEX IF EXISTS release_embedding_idx",
                    "ALTER TABLE IF EXISTS release DROP COLUMN IF EXISTS embedding",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS source_group TEXT",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS size_bytes BIGINT",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS segments JSONB",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS has_parts BOOLEAN NOT NULL DEFAULT FALSE",
                    "ALTER TABLE IF EXISTS release ADD COLUMN IF NOT EXISTS part_count INT NOT NULL DEFAULT 0",
                    "CREATE INDEX IF NOT EXISTS release_source_group_idx ON release (source_group)",
                    "CREATE INDEX IF NOT EXISTS release_size_bytes_idx ON release (size_bytes)",
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
            raise RuntimeError(
                "psycopg driver is required for PostgreSQL URLs"
            ) from exc
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
    if url == ":memory":
        raise RuntimeError("DATABASE_URL must point to a persistent database")

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
            language TEXT NOT NULL DEFAULT 'und',
            tags TEXT NOT NULL DEFAULT '',
            source_group TEXT,
            size_bytes BIGINT,
            posted_at TIMESTAMPTZ,
            segments TEXT,
            has_parts BOOLEAN NOT NULL DEFAULT 0,
            part_count INT NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_source_group_idx ON release (source_group)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_size_bytes_idx ON release (size_bytes)"
    )
    return conn


def connect_opensearch() -> Optional[object]:
    """Return an OpenSearch client if available, else None."""
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        from opensearchpy import OpenSearch  # type: ignore

        return OpenSearch(url, timeout=opensearch_timeout_seconds())
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.info(
            "OpenSearch unavailable: %s",
            str(exc),
            extra={"event": "opensearch_unavailable", "error": str(exc)},
        )
        return None


def insert_release(
    conn: Any,
    norm_title: str | None = None,
    category: Optional[str] = None,
    language: Optional[str] = None,
    tags: Optional[list[str]] = None,
    group: Optional[str] = None,
    size_bytes: Optional[int] = None,
    posted_at: Optional[str] = None,
    *,
    releases: Optional[
        Iterable[
            tuple[
                str,
                Optional[str],
                Optional[str],
                Optional[list[str]],
                Optional[str],
                Optional[int],
                Optional[str],
            ]
        ]
    ] = None,
) -> set[str]:
    """Insert one or more releases and return the inserted titles."""

    def _clean(text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        return text.encode("utf-8", "surrogateescape").decode("utf-8", "ignore")

    cur = conn.cursor()

    items: list[
        tuple[
            str,
            Optional[str],
            Optional[str],
            list[str],
            Optional[str],
            Optional[int],
            Optional[str],
        ]
    ] = []
    if releases is not None:
        for r in releases:
            n, c, lang, t, g, s, p = r
            items.append((n, c, lang, list(t or []), g, s, p))
    elif norm_title is not None:
        items.append(
            (
                norm_title,
                category,
                language,
                list(tags or []),
                group,
                size_bytes,
                posted_at,
            )
        )
    else:
        return set()

    cleaned: list[
        tuple[str, str, str, str, Optional[str], Optional[int], Optional[str]]
    ] = []
    titles: list[str] = []
    for n, c, lang, t, g, s, p in items:
        cleaned_title = _clean(n) or ""
        titles.append(cleaned_title)
        cleaned_category = _clean(c) or CATEGORY_MAP["other"]
        cleaned_language = _clean(lang) or "und"
        cleaned_tags = ",".join(_clean(tag) or "" for tag in t)
        cleaned_group = _clean(g)
        size_val = s if isinstance(s, int) and s > 0 else None
        cleaned_posted = _clean(p)
        cleaned.append(
            (
                cleaned_title,
                cleaned_category,
                cleaned_language,
                cleaned_tags,
                cleaned_group,
                size_val,
                cleaned_posted,
            )
        )

    placeholders = ",".join(
        [
            "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
            for _ in titles
        ]
    )
    existing: set[str] = set()
    if titles:
        cur.execute(
            f"SELECT norm_title FROM release WHERE norm_title IN ({placeholders})",
            titles,
        )
        existing = {row[0] for row in cur.fetchall()}

    to_insert = [row for row in cleaned if row[0] not in existing]
    inserted = {row[0] for row in to_insert}
    if to_insert:
        if conn.__class__.__module__.startswith("sqlite3"):
            cur.executemany(
                "INSERT OR IGNORE INTO release (norm_title, category, language, tags, source_group, size_bytes, posted_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                to_insert,
            )
        else:
            cur.executemany(
                "INSERT INTO release (norm_title, category, language, tags, source_group, size_bytes, posted_at) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (norm_title) DO UPDATE SET posted_at = EXCLUDED.posted_at",
                to_insert,
            )
    # Ensure posted_at is updated for existing rows
    if cleaned:
        updates = [(row[6], row[0]) for row in cleaned if row[6]]
        if updates:
            placeholder = (
                "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
            )
            cur.executemany(
                f"UPDATE release SET posted_at = {placeholder} WHERE norm_title = {placeholder}",
                updates,
            )
    conn.commit()
    return inserted


_os_warned = False


def index_release(
    client: Optional[object],
    norm_title: str,
    *,
    category: Optional[str] = None,
    language: Optional[str] = None,
    tags: Optional[list[str]] = None,
    group: Optional[str] = None,
    size_bytes: Optional[int] = None,
    posted_at: Optional[str] = None,
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
    if group:
        body["source_group"] = group
    if size_bytes is not None and size_bytes > 0:
        body["size_bytes"] = size_bytes
    if posted_at:
        body["posted_at"] = posted_at
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


def bulk_index_releases(
    client: Optional[object],
    docs: list[tuple[str, dict[str, object] | None]],
) -> None:
    """Index multiple releases into OpenSearch using the bulk API.

    Passing ``None`` as the document body issues a delete operation for the
    corresponding identifier.
    """
    global _os_warned
    if not client or not docs:
        return
    for i in range(0, len(docs), INGEST_OS_BULK):
        lines: list[str] = []
        for doc_id, body in docs[i : i + INGEST_OS_BULK]:
            if body is None:
                lines.append(
                    json.dumps({"delete": {"_index": OS_RELEASES_ALIAS, "_id": doc_id}})
                )
                continue
            lines.append(
                json.dumps({"index": {"_index": OS_RELEASES_ALIAS, "_id": doc_id}})
            )
            lines.append(json.dumps(body))
        if not lines:
            continue
        payload = "\n".join(lines) + "\n"
        try:  # pragma: no cover - network errors
            resp = client.bulk(body=payload, refresh=False)
        except Exception as exc:  # pragma: no cover - network errors
            if not _os_warned:
                logger.warning("opensearch_bulk_failed", extra={"error": str(exc)})
                _os_warned = True
        else:
            if (
                isinstance(resp, dict)
                and resp.get("errors")
                and isinstance(resp.get("items"), list)
            ):
                for item in resp["items"]:
                    info = item.get("index") or item.get("delete") or {}
                    err = info.get("error")
                    if err:
                        logger.warning(
                            "opensearch_bulk_item_failed",
                            extra={"id": info.get("_id"), "error": err.get("reason", str(err))},
                        )


def prune_group(conn: Any, client: Optional[object], group: str) -> None:
    """Remove all releases associated with ``group`` from storage."""
    cur = conn.cursor()
    placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
    cur.execute(f"DELETE FROM release WHERE source_group = {placeholder}", (group,))
    conn.commit()
    if client:
        try:  # pragma: no cover - network errors
            client.delete_by_query(
                index=OS_RELEASES_ALIAS,
                body={"query": {"term": {"source_group": group}}},
            )
        except Exception:
            logger.warning("opensearch_prune_failed", extra={"group": group})


def prune_orphaned_releases(client: Optional[object], batch: int = 1000) -> int:
    """Remove OpenSearch documents without a matching DB record.

    Returns the number of deleted documents.  ``client`` should be an
    :class:`OpenSearch`-compatible instance.  When ``client`` is ``None`` or any
    errors occur, the function exits early and returns ``0``.
    """

    if not client:
        return 0

    deleted = 0
    try:
        conn = connect_db()
    except Exception:
        logger.warning("prune_orphaned_connect_failed")
        return 0

    cur = conn.cursor()
    placeholder = "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"

    try:
        resp = client.search(
            index=OS_RELEASES_ALIAS,
            scroll="1m",
            size=batch,
            body={"query": {"match_all": {}}, "_source": False},
        )
    except Exception:
        logger.warning("opensearch_scan_failed")
        conn.close()
        return 0

    scroll_id = resp.get("_scroll_id")
    hits = resp.get("hits", {}).get("hits", [])

    try:
        while hits:
            missing: list[str] = []
            for hit in hits:
                rid = hit.get("_id")
                if rid is None:
                    continue
                cur.execute(
                    f"SELECT 1 FROM release WHERE norm_title = {placeholder}", (rid,)
                )
                if cur.fetchone() is None:
                    missing.append(rid)
            if missing:
                try:
                    client.delete_by_query(
                        index=OS_RELEASES_ALIAS,
                        body={"query": {"ids": {"values": missing}}},
                    )
                    deleted += len(missing)
                except Exception:
                    logger.warning("opensearch_delete_failed")
            resp = client.scroll(scroll_id=scroll_id, scroll="1m")
            scroll_id = resp.get("_scroll_id")
            hits = resp.get("hits", {}).get("hits", [])
    finally:
        try:
            if scroll_id:
                client.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass
        conn.close()

    return deleted


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
    install_signal_handlers()
    start_memory_logger()
    from .ingest_loop import run_forever

    run_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
