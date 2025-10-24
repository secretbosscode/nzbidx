"""Cursor tracking for NNTP groups."""

from __future__ import annotations

import logging
import os  # used to ensure path exists for SQLite databases
import sqlite3
from typing import Any, Iterable, Tuple
from urllib.parse import urlparse

from .config import CURSOR_DB

try:  # pragma: no cover - optional dependency
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore


logger = logging.getLogger(__name__)


def _conn() -> Tuple[Any, str]:
    """Return a database connection and its paramstyle."""
    parsed = urlparse(CURSOR_DB)
    if parsed.scheme.startswith("postgres"):
        if not psycopg:  # pragma: no cover - missing driver
            raise RuntimeError("psycopg is required for PostgreSQL URLs")
        url = CURSOR_DB
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        elif url.startswith("postgresql+"):
            url = url.replace("postgresql+psycopg://", "postgresql://", 1)
            url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
        conn = psycopg.connect(url)
        paramstyle = "%s"
    else:
        os.makedirs(os.path.dirname(CURSOR_DB) or ".", exist_ok=True)
        conn = sqlite3.connect(CURSOR_DB)
        paramstyle = "?"
    conn.execute(
        'CREATE TABLE IF NOT EXISTS cursor ("group" TEXT PRIMARY KEY, last_article INTEGER, irrelevant INTEGER DEFAULT 0)'
    )
    conn.execute(
        'CREATE TABLE IF NOT EXISTS cursor_meta (key TEXT PRIMARY KEY, value TEXT)'
    )
    conn.commit()
    if parsed.scheme.startswith("postgres"):
        try:
            conn.execute(
                "ALTER TABLE cursor ADD COLUMN IF NOT EXISTS irrelevant INTEGER DEFAULT 0"
            )
            conn.commit()
        except Exception:  # pragma: no cover - unexpected schema issue
            logging.exception(
                "Unexpected error adding 'irrelevant' column to cursor table"
            )
            conn.rollback()
            raise
    else:
        try:
            cur = conn.execute("PRAGMA table_info(cursor)")
            cols = [row[1] for row in cur.fetchall()]
            if "irrelevant" not in cols:
                conn.execute(
                    "ALTER TABLE cursor ADD COLUMN irrelevant INTEGER DEFAULT 0"
                )
                conn.commit()
        except sqlite3.OperationalError:
            conn.rollback()
        except Exception:  # pragma: no cover - unexpected schema issue
            logging.exception(
                "Unexpected error adding 'irrelevant' column to cursor table"
            )
            conn.rollback()
            raise
    return conn, paramstyle


def _set_group_mode(conn: Any, paramstyle: str, mode: str) -> None:
    """Persist the current group selection ``mode`` for cursor bookkeeping."""

    stmt = (
        f"INSERT INTO cursor_meta(key, value) VALUES ({paramstyle}, {paramstyle}) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
    )
    conn.execute(stmt, ("group_mode", mode))


def _get_group_mode(conn: Any) -> str | None:
    """Return the last recorded group selection mode."""

    cur = conn.execute("SELECT value FROM cursor_meta WHERE key = 'group_mode'")
    row = cur.fetchone()
    return row[0] if row else None


def get_cursors(groups: Iterable[str]) -> dict[str, int]:
    """Return the last processed article number for each ``group``."""
    group_list = list(groups)
    if not group_list:
        return {}
    conn, paramstyle = _conn()
    placeholders = ",".join([paramstyle] * len(group_list))
    cur = conn.execute(
        f'SELECT "group", last_article FROM cursor WHERE "group" IN ({placeholders}) AND irrelevant = 0',
        tuple(group_list),
    )
    rows = cur.fetchall()
    conn.close()
    return {row[0]: int(row[1]) for row in rows}


def get_cursor(group: str) -> int | None:
    """Return the last processed article number for ``group``."""
    return get_cursors([group]).get(group)


def set_cursors(updates: dict[str, int]) -> None:
    """Persist ``last_article`` cursors for multiple groups."""
    if not updates:
        return
    conn, paramstyle = _conn()
    stmt = (
        f'INSERT INTO cursor("group", last_article, irrelevant) '
        f"VALUES ({paramstyle}, {paramstyle}, 0) "
        'ON CONFLICT("group") DO UPDATE SET last_article=excluded.last_article, irrelevant=0'
    )
    errors: tuple[type[Exception], ...]
    if psycopg:
        errors = (sqlite3.OperationalError, psycopg.Error)
    else:  # pragma: no cover - optional dependency
        errors = (sqlite3.OperationalError,)
    try:
        try:
            with conn.cursor() as cur:
                cur.executemany(stmt, [(g, c) for g, c in updates.items()])
        except (AttributeError, TypeError):
            cur = conn.cursor()
            try:
                cur.executemany(stmt, [(g, c) for g, c in updates.items()])
            finally:
                cur.close()
        conn.commit()
    except errors as exc:
        logger.exception("cursor_update_failed")
        conn.rollback()
        raise RuntimeError("Failed to set cursors") from exc
    finally:
        conn.close()


def set_cursor(group: str, last_article: int) -> None:
    """Persist the ``last_article`` cursor for ``group``."""
    set_cursors({group: last_article})


def mark_irrelevant(group: str) -> None:
    """Mark ``group`` as irrelevant to skip future processing."""
    conn, paramstyle = _conn()
    conn.execute(
        f'INSERT INTO cursor("group", last_article, irrelevant) VALUES ({paramstyle}, 0, 1) '
        'ON CONFLICT("group") DO UPDATE SET irrelevant=1',
        (group,),
    )
    conn.commit()
    conn.close()


def get_irrelevant_groups() -> list[str]:
    """Return all groups marked as irrelevant."""
    conn, _ = _conn()
    cur = conn.execute('SELECT "group" FROM cursor WHERE irrelevant = 1')
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]


def reset(allowed: Iterable[str] | None = None) -> None:
    """Reset cursor state, optionally keeping entries for ``allowed`` groups."""

    conn, paramstyle = _conn()
    try:
        if allowed:
            allowed_set = sorted({g for g in allowed if g})
        else:
            allowed_set = []
        if not allowed_set:
            conn.execute('DELETE FROM cursor')
        else:
            placeholders = ",".join([paramstyle] * len(allowed_set))
            conn.execute(
                f'DELETE FROM cursor WHERE "group" NOT IN ({placeholders})',
                tuple(allowed_set),
            )
        conn.commit()
    finally:
        conn.close()


def reset_for_curated() -> bool:
    """Clear cursor state when switching into curated mode.

    Returns ``True`` if a reset occurred.
    """

    conn, paramstyle = _conn()
    try:
        current = _get_group_mode(conn)
        if current == "curated":
            return False
        conn.execute('DELETE FROM cursor')
        _set_group_mode(conn, paramstyle, "curated")
        conn.commit()
        return True
    finally:
        conn.close()


def mark_group_mode(mode: str) -> None:
    """Record the active group selection ``mode`` without mutating cursors."""

    conn, paramstyle = _conn()
    try:
        _set_group_mode(conn, paramstyle, mode)
        conn.commit()
    finally:
        conn.close()
