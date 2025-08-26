"""Cursor tracking for NNTP groups."""

from __future__ import annotations

import atexit
import os  # used to ensure path exists for SQLite databases
import sqlite3
from typing import Any, Tuple
from urllib.parse import urlparse

from .config import CURSOR_DB

try:  # pragma: no cover - optional dependency
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore


_CONN: Any | None = None
_PARAMSTYLE: str | None = None


def _close_conn() -> None:
    """Close the global connection if it exists."""
    global _CONN
    if _CONN is not None:
        _CONN.close()
        _CONN = None


def _get_conn() -> Tuple[Any, str]:
    """Return a module-level database connection and its paramstyle."""
    global _CONN, _PARAMSTYLE
    if _CONN is not None and _PARAMSTYLE is not None:
        return _CONN, _PARAMSTYLE

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
    try:
        conn.execute("ALTER TABLE cursor ADD COLUMN irrelevant INTEGER DEFAULT 0")
        conn.commit()
    except Exception:  # column already exists
        conn.rollback()

    _CONN, _PARAMSTYLE = conn, paramstyle
    atexit.register(_close_conn)
    return conn, paramstyle


def _conn() -> Tuple[Any, str]:
    """Backward compatible alias for tests expecting ``_conn``."""
    return _get_conn()


def get_cursor(group: str) -> int | None:
    """Return the last processed article number for ``group``."""
    conn, paramstyle = _get_conn()
    cur = conn.execute(
        f'SELECT last_article FROM cursor WHERE "group" = {paramstyle} AND irrelevant = 0',
        (group,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def set_cursor(group: str, last_article: int) -> None:
    """Persist the ``last_article`` cursor for ``group``."""
    conn, paramstyle = _get_conn()
    conn.execute(
        f'INSERT INTO cursor("group", last_article, irrelevant) VALUES ({paramstyle}, {paramstyle}, 0) '
        'ON CONFLICT("group") DO UPDATE SET last_article=excluded.last_article, irrelevant=0',
        (group, last_article),
    )
    conn.commit()


def mark_irrelevant(group: str) -> None:
    """Mark ``group`` as irrelevant to skip future processing."""
    conn, paramstyle = _get_conn()
    conn.execute(
        f'INSERT INTO cursor("group", last_article, irrelevant) VALUES ({paramstyle}, 0, 1) '
        'ON CONFLICT("group") DO UPDATE SET irrelevant=1',
        (group,),
    )
    conn.commit()


def get_irrelevant_groups() -> list[str]:
    """Return all groups marked as irrelevant."""
    conn, _ = _get_conn()
    cur = conn.execute('SELECT "group" FROM cursor WHERE irrelevant = 1')
    rows = cur.fetchall()
    return [row[0] for row in rows]
