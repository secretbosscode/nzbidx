"""SQL helpers."""

from __future__ import annotations

from typing import Any


def sql_placeholder(conn: Any) -> str:
    """Return DB-API placeholder style for connection object.

    Uses ``?`` for sqlite3 connections and ``%s`` for others.
    """
    return "?" if conn.__class__.__module__.startswith("sqlite3") else "%s"
