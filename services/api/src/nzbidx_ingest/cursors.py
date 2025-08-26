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
    conn.commit()
    db_specific_errors: tuple[type[BaseException], ...] = (sqlite3.OperationalError,)
    if psycopg:
        db_specific_errors += (psycopg.errors.DuplicateColumn,)
    try:
        conn.execute("ALTER TABLE cursor ADD COLUMN irrelevant INTEGER DEFAULT 0")
        conn.commit()
    except db_specific_errors:
        conn.rollback()
    except Exception:  # pragma: no cover - unexpected schema issue
        logging.exception("Unexpected error adding 'irrelevant' column to cursor table")
        conn.rollback()
        raise
    return conn, paramstyle


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
