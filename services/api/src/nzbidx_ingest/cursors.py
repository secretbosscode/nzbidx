"""Cursor tracking for NNTP groups."""

from __future__ import annotations

import sqlite3

from .config import CURSOR_DB


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(CURSOR_DB)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cursor (group TEXT PRIMARY KEY, last_article INTEGER)"
    )
    return conn


def get_cursor(group: str) -> int | None:
    """Return the last processed article number for ``group``."""
    conn = _conn()
    cur = conn.execute("SELECT last_article FROM cursor WHERE group = ?", (group,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def set_cursor(group: str, last_article: int) -> None:
    """Persist the ``last_article`` cursor for ``group``."""
    conn = _conn()
    conn.execute(
        "INSERT INTO cursor(group, last_article) VALUES (?, ?) "
        "ON CONFLICT(group) DO UPDATE SET last_article=excluded.last_article",
        (group, last_article),
    )
    conn.commit()
    conn.close()
