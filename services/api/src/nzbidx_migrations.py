from __future__ import annotations

import logging
import re
from importlib import resources
from typing import Any, Callable

try:  # pragma: no cover - optional dependency
    import sqlparse
except Exception:  # pragma: no cover - optional dependency
    sqlparse = None  # type: ignore

logger = logging.getLogger(__name__)


def _split_sql(sql: str) -> list[str]:
    """Split SQL statements by semicolons while respecting quotes."""

    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    dollars: list[str] = []
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_single:
            buf.append(ch)
            if ch == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(ch)
            if ch == '"':
                in_double = False
            i += 1
            continue
        if dollars:
            buf.append(ch)
            if sql.startswith(dollars[-1], i):
                i += len(dollars[-1])
                dollars.pop()
            else:
                i += 1
            continue
        if ch == "'":
            in_single = True
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            buf.append(ch)
            i += 1
            continue
        if ch == "$":
            m = re.match(r"\$[A-Za-z0-9_]*\$", sql[i:])
            if m:
                tag = m.group(0)
                dollars.append(tag)
                buf.append(tag)
                i += len(tag)
                continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf.clear()
            i += 1
            continue
        buf.append(ch)
        i += 1
    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)
    return statements


def load_schema_statements() -> list[str]:
    """Return SQL statements from the bundled schema file."""
    sql = (
        resources.files("nzbidx_api").joinpath("schema.sql").read_text(encoding="utf-8")
    )
    if sqlparse:
        return [s.strip() for s in sqlparse.split(sql) if s.strip()]
    return _split_sql(sql)


def apply_sync(
    conn: Any,
    text_fn: Any,
    predicate: Callable[[str], bool] | None = None,
    statements: list[str] | None = None,
) -> None:
    """Apply schema statements using a synchronous connection."""
    for stmt in statements or load_schema_statements():
        if predicate and not predicate(stmt):
            continue
        try:
            conn.execute(text_fn(stmt))
            conn.commit()
        except Exception as exc:  # pragma: no cover - best effort logging
            conn.rollback()
            if stmt.lstrip().upper().startswith("CREATE EXTENSION"):
                logger.warning(
                    "extension_unavailable", extra={"stmt": stmt, "error": str(exc)}
                )
            else:
                raise


async def apply_async(
    conn: Any,
    text_fn: Any,
    statements: list[str] | None = None,
    predicate: Callable[[str], bool] | None = None,
) -> None:
    """Apply schema statements using an async connection."""
    for stmt in statements or load_schema_statements():
        if predicate and not predicate(stmt):
            continue
        try:
            await conn.execute(text_fn(stmt))
            await conn.commit()
        except Exception as exc:  # pragma: no cover - best effort logging
            await conn.rollback()
            if stmt.lstrip().upper().startswith("CREATE EXTENSION"):
                logger.warning(
                    "extension_unavailable", extra={"stmt": stmt, "error": str(exc)}
                )
            else:
                raise
