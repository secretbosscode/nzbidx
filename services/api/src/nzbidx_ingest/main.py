"""Entry point for the ingest worker."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Optional, Any, Iterable
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone

try:  # pragma: no cover - optional dependency
    from dateutil import parser as dateutil_parser
except Exception:  # pragma: no cover - optional dependency
    dateutil_parser = None  # type: ignore

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


from .logging import setup_logging
from .parsers import extract_tags
from .resource_monitor import install_signal_handlers, start_memory_logger
from .db_migrations import (
    migrate_release_table,
    ensure_release_adult_year_partition,
    migrate_release_adult_partitions,
)
from nzbidx_api.db import sql_placeholder
from nzbidx_migrations import apply_sync

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


def _load_group_category_hints() -> list[tuple[str, str]]:
    """Return default group/category hints, optionally extended via config."""
    hints: list[tuple[str, str]] = [
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
        ("hentai", "xxx"),
        ("animation", "xxx"),
        ("erotica", "xxx"),
        ("erotic", "xxx"),
        ("porn", "xxx"),
        ("porno", "xxx"),
        ("comics", "comics"),
        ("comic", "comics"),
        ("misc", "misc"),
        ("other", "other"),
    ]

    cfg = os.getenv("GROUP_CATEGORY_HINTS_FILE")
    if cfg:
        try:
            data = json.loads(Path(cfg).read_text())
            if isinstance(data, dict):
                extra = [(k, v) for k, v in data.items()]
            else:
                extra = [
                    tuple(item)
                    for item in data
                    if isinstance(item, (list, tuple)) and len(item) == 2
                ]
            hints.extend([(str(k), str(v)) for k, v in extra])
            logger.info(
                "group_category_hints_loaded",
                extra={
                    "event": "group_category_hints_loaded",
                    "path": cfg,
                    "count": len(extra),
                },
            )
        except Exception:
            logger.warning(
                "group_category_hints_load_failed",
                extra={"event": "group_category_hints_load_failed", "path": cfg},
            )
    return hints


GROUP_CATEGORY_HINTS: list[tuple[str, str]] = _load_group_category_hints()


DEFAULT_ADULT_KEYWORDS = (
    "brazzers",
    "realitykings",
    "onlyfans",
    "pornhub",
    "adult",
    "xxx",
    "porn",
    "erotica",
    "nsfw",
)
ADULT_KEYWORDS = tuple(
    k.strip().lower()
    for k in os.getenv("ADULT_KEYWORDS", ",".join(DEFAULT_ADULT_KEYWORDS)).split(",")
    if k.strip()
)

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

            # Ensure the ``release`` table is partitioned before attempting any
            # migrations.  If the table exists but is not partitioned, attempt to
            # migrate it automatically and verify the result.
            raw = None
            try:
                raw = engine.raw_connection()
                cur = raw.cursor()
                cur.execute(
                    "SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'release')"
                )
                exists = bool(cur.fetchone()[0])
                cur.execute(
                    "SELECT EXISTS ("
                    "SELECT FROM pg_partitioned_table WHERE partrelid = 'release'::regclass"
                    ")"
                )
                partitioned = bool(cur.fetchone()[0])
                if exists and not partitioned:
                    logger.info("release_table_migrating")
                    try:
                        migrate_release_table(raw)
                    except Exception:
                        pass
                    cur = raw.cursor()
                    cur.execute(
                        "SELECT EXISTS ("
                        "SELECT FROM pg_partitioned_table WHERE partrelid = 'release'::regclass"
                        ")"
                    )
                    partitioned = bool(cur.fetchone()[0])
                    if not partitioned:
                        logger.error("release_table_not_partitioned")
                        raise RuntimeError("release table must be partitioned")
                cur = raw.cursor()
                cur.execute(
                    "SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'release_adult')"
                )
                adult_exists = bool(cur.fetchone()[0])
                cur.execute(
                    """
                        SELECT EXISTS(
                            SELECT 1
                            FROM pg_partitioned_table p
                            JOIN pg_class c ON p.partrelid = c.oid
                            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(p.partattrs)
                            WHERE c.relname = 'release_adult' AND a.attname = 'posted_at'
                        )
                    """
                )
                adult_partitioned = bool(cur.fetchone()[0])
                if adult_exists and not adult_partitioned:
                    logger.info("release_adult_table_migrating")
                    try:
                        migrate_release_adult_partitions(raw)
                    except Exception:
                        pass
                    cur = raw.cursor()
                    cur.execute(
                        """
                            SELECT EXISTS(
                                SELECT 1
                                FROM pg_partitioned_table p
                                JOIN pg_class c ON p.partrelid = c.oid
                                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(p.partattrs)
                                WHERE c.relname = 'release_adult' AND a.attname = 'posted_at'
                            )
                        """
                    )
                    adult_partitioned = bool(cur.fetchone()[0])
                    if adult_partitioned:
                        logger.info("release_adult_table_auto_migrated")
                if adult_exists and not adult_partitioned:
                    logger.error("release_adult_table_not_partitioned")
                    raise RuntimeError(
                        "release_adult table must be partitioned by posted_at"
                    )
            except Exception:
                # On any errors (e.g. system catalogs missing) fall back to the
                # migration logic below which will attempt to create the
                # required structures.  Any failures there will surface as
                # RuntimeError from the caller's perspective.
                pass
            finally:
                if raw is not None:
                    try:
                        raw.close()
                    except Exception:
                        pass

            with engine.connect() as conn:  # type: ignore[call-arg]
                exists = conn.execute(
                    text("SELECT EXISTS (SELECT FROM pg_class WHERE relname='release')")
                ).fetchone()[0]
                partitioned = conn.execute(
                    text(
                        """
                            SELECT EXISTS(
                                SELECT 1
                                FROM pg_partitioned_table p
                                JOIN pg_class c ON p.partrelid = c.oid
                                WHERE c.relname = 'release'
                            )
                            """
                    )
                ).fetchone()[0]
                adult_exists = conn.execute(
                    text(
                        "SELECT EXISTS (SELECT FROM pg_class WHERE relname='release_adult')"
                    )
                ).fetchone()[0]
                adult_partitioned = conn.execute(
                    text(
                        """
                            SELECT EXISTS(
                                SELECT 1
                                FROM pg_partitioned_table p
                                JOIN pg_class c ON p.partrelid = c.oid
                                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(p.partattrs)
                                WHERE c.relname = 'release_adult' AND a.attname = 'posted_at'
                            )
                        """
                    )
                ).fetchone()[0]
                if exists and not partitioned:
                    logger.error(
                        "release_table_not_partitioned",
                        extra={"next_step": "drop_or_migrate"},
                    )
                    raise RuntimeError(
                        "'release' table exists but is not partitioned; drop or migrate the table before starting the worker"
                    )
                if adult_exists and not adult_partitioned:
                    logger.error(
                        "release_adult_table_not_partitioned",
                        extra={"next_step": "drop_or_migrate"},
                    )
                    raise RuntimeError(
                        "'release_adult' table exists but is not partitioned by posted_at; drop or migrate the table before starting the worker"
                    )
                if not exists:
                    logger.info(
                        "release_table_missing",
                        extra={"next_step": "creating"},
                    )
                else:
                    logger.info(
                        "release_table_partitioned",
                        extra={"next_step": "ensuring_partitions"},
                    )

                if not exists or partitioned:
                    apply_sync(conn, text)
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
            engine = create_engine(
                admin_url, echo=False, future=True, isolation_level="AUTOCOMMIT"
            )
            with engine.connect() as conn:  # type: ignore[call-arg]
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
            norm_title TEXT,
            category TEXT,
            category_id INT,
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_category_id_idx ON release (category_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_norm_title_idx ON release (norm_title)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS release_tags_idx ON release (tags)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS release_has_parts_idx ON release (posted_at) WHERE has_parts"
    )
    return conn


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
    placeholder = sql_placeholder(conn)

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
        tuple[
            str,
            str,
            int,
            str,
            str,
            Optional[str],
            Optional[int],
            Optional[datetime],
        ]
    ] = []
    titles: list[str] = []
    for n, c, lang, t, g, s, p in items:
        cleaned_title = _clean(n) or ""
        titles.append(cleaned_title)
        cleaned_category = _clean(c) or CATEGORY_MAP["other"]
        try:
            cleaned_category_id = int(cleaned_category)
        except ValueError:
            cleaned_category_id = int(CATEGORY_MAP["other"])
        cleaned_language = _clean(lang) or "und"
        cleaned_tags = ",".join(_clean(tag) or "" for tag in t)
        cleaned_group = _clean(g)
        size_val = s if isinstance(s, int) and s > 0 else None
        cleaned_posted_str = _clean(p)
        cleaned_posted: Optional[datetime] = None
        if cleaned_posted_str:
            try:
                cleaned_posted = datetime.fromisoformat(cleaned_posted_str)
            except ValueError:
                if dateutil_parser is not None:
                    try:
                        cleaned_posted = dateutil_parser.parse(cleaned_posted_str)
                    except Exception:
                        cleaned_posted = None
            if cleaned_posted and cleaned_posted.tzinfo is None:
                cleaned_posted = cleaned_posted.replace(tzinfo=timezone.utc)
        cleaned.append(
            (
                cleaned_title,
                cleaned_category,
                cleaned_category_id,
                cleaned_language,
                cleaned_tags,
                cleaned_group,
                size_val,
                cleaned_posted,
            )
        )

    placeholders = ",".join([placeholder] * len(titles))
    existing: set[tuple[str, int]] = set()
    updates: list[tuple[Optional[datetime], str, int]] = []
    if titles:
        cur.execute(
            f"SELECT norm_title, category_id FROM release WHERE norm_title IN ({placeholders})",
            titles,
        )
        existing = {(row[0], row[1]) for row in cur.fetchall()}

    to_insert: list[
        tuple[str, str, int, str, str, Optional[str], Optional[int], Optional[datetime]]
    ] = []
    for row in cleaned:
        key = (row[0], row[2])
        if key not in existing:
            to_insert.append(row)
            existing.add(key)
        elif row[7]:
            updates.append((row[7], row[0], row[2]))
    inserted = {row[0] for row in to_insert}
    if to_insert:
        if placeholder == "?":
            sqlite_rows = [
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7].isoformat() if row[7] else None,
                )
                for row in to_insert
            ]
            cur.executemany(
                "INSERT OR IGNORE INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, posted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                sqlite_rows,
            )
        else:
            adult_rows: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
            other_rows: list[tuple[Any, ...]] = []
            for row in to_insert:
                category_id = row[2]
                posted_at = row[7]
                if (
                    category_id is not None
                    and 6000 <= category_id < 7000
                    and posted_at is not None
                ):
                    adult_rows[posted_at.year].append(row)
                else:
                    other_rows.append(row)
            for year, rows in adult_rows.items():
                ensure_release_adult_year_partition(conn, year)
                cur.executemany(
                    f"INSERT INTO release_adult_{year} (norm_title, category, category_id, language, tags, source_group, size_bytes, posted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (norm_title, category_id, posted_at) DO NOTHING",
                    rows,
                )
            if other_rows:
                cur.executemany(
                    "INSERT INTO release (norm_title, category, category_id, language, tags, source_group, size_bytes, posted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (norm_title, category_id, posted_at) DO NOTHING",
                    other_rows,
                )
    # Ensure posted_at is updated for existing rows
    if updates:
        if placeholder == "?":
            sqlite_updates = [
                (u[0].isoformat() if u[0] else None, u[1], u[2]) for u in updates
            ]
            cur.executemany(
                f"UPDATE release SET posted_at = {placeholder} WHERE norm_title = {placeholder} AND category_id = {placeholder}",
                sqlite_updates,
            )
        else:
            cur.executemany(
                f"UPDATE release SET posted_at = {placeholder} WHERE norm_title = {placeholder} AND category_id = {placeholder}",
                updates,
            )
    conn.commit()
    return inserted


def prune_group(conn: Any, group: str) -> None:
    """Remove all releases associated with ``group`` from storage."""
    cur = conn.cursor()
    placeholder = sql_placeholder(conn)
    cur.execute(f"DELETE FROM release WHERE source_group = {placeholder}", (group,))
    conn.commit()


def _infer_category(subject: str, group: Optional[str] = None) -> Optional[str]:
    """Heuristic category detection from the raw subject or group."""
    s = subject.lower()

    if group:
        g = group.lower()
        for key, cat in GROUP_CATEGORY_HINTS:
            if key in g:
                if cat == "xxx":
                    if "dvd" in s:
                        return CATEGORY_MAP["xxx_dvd"]
                    if "wmv" in s:
                        return CATEGORY_MAP["xxx_wmv"]
                    if "xvid" in s:
                        return CATEGORY_MAP["xxx_xvid"]
                    if "x264" in s or "h264" in s:
                        return CATEGORY_MAP["xxx_x264"]
                    return CATEGORY_MAP["xxx"]
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
    if any(k in s for k in ADULT_KEYWORDS):
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
