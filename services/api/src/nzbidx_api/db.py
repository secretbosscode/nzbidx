"""Database utilities for the API service."""

from __future__ import annotations

import asyncio
import logging
import os
from importlib import resources
from urllib.parse import urlparse, urlunparse

from typing import Any, Optional

# Optional SQLAlchemy dependency
try:  # pragma: no cover - import guard
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore
    AsyncEngine = None  # type: ignore
    create_async_engine = None  # type: ignore

# Optional sqlparse dependency for parsing schema statements
try:  # pragma: no cover - import guard
    import sqlparse
except Exception:  # pragma: no cover - optional dependency
    sqlparse = None  # type: ignore

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://localhost:5432/postgres")
DATABASE_URL = os.path.expandvars(DATABASE_URL)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

if create_async_engine:
    engine: Optional[AsyncEngine] = create_async_engine(DATABASE_URL, echo=False)
else:  # pragma: no cover - no sqlalchemy available
    engine = None


def _split_sql(sql: str) -> list[str]:
    """Split SQL script into individual statements.

    When ``sqlparse`` is available, it is used for correctness.  Otherwise a
    tiny state machine is employed which understands basic quoting rules and
    PostgreSQL dollar-quoted blocks so that semicolons inside functions or
    strings do not prematurely terminate statements.
    """

    if sqlparse:  # pragma: no cover - exercised when dependency installed
        return [s.strip() for s in sqlparse.split(sql) if s.strip()]

    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    dollar_tag: str | None = None
    i = 0
    length = len(sql)
    while i < length:
        ch = sql[i]
        if in_single:
            current.append(ch)
            if ch == "'":
                # handle escaped '' inside strings
                if i + 1 < length and sql[i + 1] == "'":
                    current.append("'")
                    i += 1
                else:
                    in_single = False
            i += 1
            continue
        if in_double:
            current.append(ch)
            if ch == '"':
                if i + 1 < length and sql[i + 1] == '"':
                    current.append('"')
                    i += 1
                else:
                    in_double = False
            i += 1
            continue
        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
            else:
                current.append(ch)
                i += 1
            continue

        # Outside of any quoted context
        if ch == "'":
            in_single = True
            current.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            current.append(ch)
            i += 1
            continue
        if ch == "$":
            j = i + 1
            while j < length and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < length and sql[j] == "$":
                dollar_tag = sql[i : j + 1]
                current.append(dollar_tag)
                i = j + 1
                continue
        if ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1

    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


async def apply_schema(max_attempts: int = 5, retry_delay: float = 1.0) -> None:
    """Create database schema if it does not already exist."""
    if not engine or not text:
        return
    sql = (
        resources.files(__package__).joinpath("schema.sql").read_text(encoding="utf-8")
    )
    statements = _split_sql(sql)

    async def _apply(conn: Any) -> None:
        for stmt in statements:
            try:
                await conn.execute(text(stmt))
                await conn.commit()
            except Exception as exc:
                # Creating extensions requires superuser privileges.  If the
                # current role lacks permission, log the failure but continue
                # applying the remaining schema.  Roll back the failed
                # statement so subsequent statements can proceed.
                await conn.rollback()
                if stmt.lstrip().upper().startswith("CREATE EXTENSION"):
                    logger.warning(
                        "extension_unavailable", extra={"stmt": stmt, "error": str(exc)}
                    )
                else:
                    raise

    async def _drop_privileges(conn: Any) -> None:
        """Revoke superuser rights from the current role if possible."""
        try:
            await conn.execute(text("ALTER ROLE CURRENT_USER NOSUPERUSER"))
            await conn.commit()
        except Exception:
            await conn.rollback()

    for attempt in range(1, max_attempts + 1):
        try:
            async with engine.connect() as conn:
                await _apply(conn)
                await _drop_privileges(conn)
            return
        except OSError as exc:
            logger.warning(
                "database_unavailable",
                extra={
                    "error": str(exc),
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                },
            )
            if attempt == max_attempts:
                return
            await asyncio.sleep(retry_delay)
        except Exception as exc:
            msg = str(getattr(exc, "orig", exc)).lower()
            if "does not exist" not in msg and "invalid catalog name" not in msg:
                raise
            await _create_database(DATABASE_URL)
            # Retry after creating the database
            continue


async def _create_database(url: str) -> None:
    """Create the PostgreSQL database referenced by ``url`` if missing."""
    if not create_async_engine or not text:
        return
    parsed = urlparse(url)
    if not parsed.scheme.startswith("postgres"):
        return
    dbname = parsed.path.lstrip("/")
    admin_url = urlunparse(parsed._replace(path="/postgres"))
    admin_engine = create_async_engine(
        admin_url, echo=False, isolation_level="AUTOCOMMIT"
    )
    try:
        async with admin_engine.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname=:name"),
                {"name": dbname},
            )
            if not exists:
                try:
                    await conn.execute(text(f'CREATE DATABASE "{dbname}"'))
                except Exception as exc:  # pragma: no cover - db may exist
                    msg = str(getattr(exc, "orig", exc)).lower()
                    if "already exists" not in msg and "duplicate database" not in msg:
                        raise
    finally:
        await admin_engine.dispose()


async def ping() -> bool:
    """Check database connectivity."""
    if not engine or not text:
        return False
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # pragma: no cover - network errors
        return False


async def _maintenance(stmt: str) -> None:
    """Execute a maintenance statement with autocommit."""
    if not engine or not text:
        return
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(stmt))


async def vacuum_analyze(table: str | None = None) -> None:
    """Run ``VACUUM (ANALYZE)`` on the database or a specific table."""
    stmt = "VACUUM (ANALYZE)"
    if table:
        stmt += f" {table}"
    await _maintenance(stmt)


async def reindex(table: str | None = None) -> None:
    """Run ``REINDEX`` for the whole database or a given table."""
    stmt = "REINDEX"
    if table:
        stmt += f" TABLE {table}"
    await _maintenance(stmt)


async def analyze(table: str | None = None) -> None:
    """Update planner statistics for all tables or a specific table."""
    stmt = "ANALYZE"
    if table:
        stmt += f" {table}"
    await _maintenance(stmt)


# ---------------------------------------------------------------------------
# Synchronous connection helpers
# ---------------------------------------------------------------------------

_conn: Optional[Any] = None


def get_connection() -> Any:
    """Return a persistent database connection for synchronous callers."""

    global _conn
    if _conn is None:
        from nzbidx_ingest.main import connect_db  # type: ignore

        _conn = connect_db()
    return _conn


def close_connection() -> None:
    """Close the persistent synchronous connection if it exists."""

    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
