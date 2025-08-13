"""Cursor tracking for NNTP groups."""

from __future__ import annotations

import os
import sqlite3
from typing import Any
from urllib.parse import urlparse

from .config import CURSOR_DB

try:  # pragma: no cover - optional dependency
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

_PARAMSTYLE = "?"


def _conn() -> Any:
    global _PARAMSTYLE
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
        _PARAMSTYLE = "%s"
    else:
        os.makedirs(os.path.dirname(CURSOR_DB) or ".", exist_ok=True)
        conn = sqlite3.connect(CURSOR_DB)
        _PARAMSTYLE = "?"
    conn.execute(
        'CREATE TABLE IF NOT EXISTS cursor ("group" TEXT PRIMARY KEY, last_article INTEGER)'
    )
    return conn


def get_cursor(group: str) -> int | None:
    """Return the last processed article number for ``group``."""
    conn = _conn()
    cur = conn.execute(
        f'SELECT last_article FROM cursor WHERE "group" = {_PARAMSTYLE}', (group,)
    )
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def set_cursor(group: str, last_article: int) -> None:
    """Persist the ``last_article`` cursor for ``group``."""
    conn = _conn()
    conn.execute(
        f'INSERT INTO cursor("group", last_article) VALUES ({_PARAMSTYLE}, {_PARAMSTYLE}) '
        'ON CONFLICT("group") DO UPDATE SET last_article=excluded.last_article',
        (group, last_article),
    )
    conn.commit()
    conn.close()
