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

# Optional sqlparse dependency for parsing schema statements.  When unavailable
# a lightweight internal splitter handles common PostgreSQL quoting constructs.
try:  # pragma: no cover - import guard
    import sqlparse
except Exception:  # pragma: no cover - optional dependency
    sqlparse = None  # type: ignore

# Optional psycopg dependency for synchronous connection handling.
try:  # pragma: no cover - import guard
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

if psycopg:  # pragma: no cover - psycopg not installed
    DB_CLOSE_ERRORS = (psycopg.Error,)
else:  # pragma: no cover - psycopg not installed
    DB_CLOSE_ERRORS: tuple[type[BaseException], ...] = ()

# Optional asyncpg dependency for low-level connection termination.
try:  # pragma: no cover - import guard
    from asyncpg.exceptions import InternalClientError
except Exception:  # pragma: no cover - optional dependency
    InternalClientError = RuntimeError  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://localhost:5432/postgres")
DATABASE_URL = os.path.expandvars(DATABASE_URL)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Engine lifecycle management -------------------------------------------------
if create_async_engine:
    POOL_RECYCLE_SECONDS = int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800"))
else:  # pragma: no cover - no sqlalchemy available
    POOL_RECYCLE_SECONDS = 0

_engine: Optional[AsyncEngine] = None
_engine_loop: Optional[asyncio.AbstractEventLoop] = None


async def init_engine() -> None:
    """Initialize the async engine bound to the current event loop.

    Any existing engine is disposed on its original loop; if that loop has
    been closed, disposal falls back to the current loop and a warning is
    emitted.
    """

    if not create_async_engine:
        return

    global _engine, _engine_loop
    loop = asyncio.get_running_loop()
    if _engine is not None:
        if _engine_loop is loop:
            return
        await dispose_engine()

    _engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=POOL_RECYCLE_SECONDS,
    )
    _engine_loop = loop


def get_engine() -> Optional[AsyncEngine]:
    """Return the loop-bound engine instance if initialized."""

    return _engine


def _split_sql(sql: str) -> list[str]:
    """Split ``sql`` into individual statements.

    The implementation intentionally handles only a small subset of PostgreSQL
    syntax but correctly preserves semicolons that appear inside quoted strings
    or dollar-quoted PL/pgSQL blocks.  It also skips over line comments (``--``)
    and C-style block comments (``/* ... */``) so that any semicolons contained
    within them do not terminate a statement.  The function is designed as a
    lightweight fallback when :mod:`sqlparse` is not available.
    """

    import re

    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    in_double = False
    line_comment = False
    block_comment = False
    dollars: list[str] = []

    while i < n:
        ch = sql[i]
        nxt = sql[i : i + 2]

        if line_comment:
            buf.append(ch)
            i += 1
            if ch == "\n":
                line_comment = False
            continue
        if block_comment:
            buf.append(ch)
            i += 1
            if nxt == "*/":
                buf.append("/")
                i += 1
                block_comment = False
            continue
        if dollars:
            tag = dollars[-1]
            if sql.startswith(tag, i):
                buf.append(tag)
                i += len(tag)
                dollars.pop()
                continue
            buf.append(ch)
            i += 1
            continue
        if in_single:
            buf.append(ch)
            i += 1
            if ch == "'":
                in_single = False
            continue
        if in_double:
            buf.append(ch)
            i += 1
            if ch == '"':
                in_double = False
            continue

        if nxt == "--":
            line_comment = True
            buf.append(nxt)
            i += 2
            continue
        if nxt == "/*":
            block_comment = True
            buf.append(nxt)
            i += 2
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


async def apply_schema(max_attempts: int = 5, retry_delay: float = 1.0) -> None:
    """Create database schema if it does not already exist."""
    engine = get_engine()
    if not engine or not text:
        return
    sql = (
        resources.files(__package__).joinpath("schema.sql").read_text(encoding="utf-8")
    )
    if sqlparse:
        statements = [s.strip() for s in sqlparse.split(sql) if s.strip()]
    else:  # pragma: no cover - sqlparse not installed
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
    engine = get_engine()
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
    engine = get_engine()
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
    from nzbidx_ingest.main import connect_db  # type: ignore

    reconnect: Optional[str] = None
    if _conn is None:
        reconnect = "init"
    else:
        if getattr(_conn, "closed", False):
            reconnect = "closed"
        else:
            cur = None
            try:
                cur = _conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            except Exception:
                reconnect = "error"
            finally:
                if cur is not None:
                    try:
                        cur.close()
                    except Exception:
                        pass

    if reconnect:
        if reconnect != "init":
            logger.info("database_reconnecting", extra={"reason": reconnect})
            try:
                _conn.close()
            except Exception:
                pass
        _conn = connect_db()
    return _conn


def close_connection() -> None:
    """Close the persistent synchronous connection if it exists."""

    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except DB_CLOSE_ERRORS:
            logger.warning("connection_close_failed", exc_info=True)
            _conn = None
        else:
            _conn = None


async def dispose_engine() -> None:
    """Dispose the global async engine and close pooled connections.

    If the engine was created on a different or closed loop the disposal
    falls back to the current loop.  When the original loop has already been
    closed the engine's pooled connections are terminated directly to avoid
    awaiting on a defunct loop.
    """

    global _engine, _engine_loop
    if _engine is None:
        return
    loop = asyncio.get_running_loop()
    try:
        if _engine_loop is loop:
            await _engine.dispose()
        elif _engine_loop and not _engine_loop.is_closed():
            try:
                fut = asyncio.run_coroutine_threadsafe(_engine.dispose(), _engine_loop)
            except RuntimeError:
                await _engine.dispose()
            else:

                def _log_disposal_result(f: "asyncio.Future[Any]") -> None:
                    try:
                        f.result()
                    except Exception:
                        logger.exception("engine_dispose_failed")

                fut.add_done_callback(_log_disposal_result)
                await asyncio.wrap_future(fut)
        elif _engine_loop and _engine_loop.is_closed():
            pool = getattr(getattr(_engine, "sync_engine", None), "pool", None)
            if pool is not None:
                raw_pool = getattr(pool, "_pool", None)
                if raw_pool is not None:
                    while True:
                        try:
                            rec = raw_pool.get_nowait()
                        except Exception:
                            break
                        dbapi_conn = getattr(
                            rec, "dbapi_connection", getattr(rec, "connection", None)
                        )
                        raw_conn = getattr(dbapi_conn, "_connection", dbapi_conn)
                        proto = getattr(raw_conn, "_protocol", None)
                        if proto is not None:
                            closer = getattr(
                                proto,
                                "close_transport",
                                getattr(proto, "terminate", None),
                            )
                            if callable(closer):
                                conn_id = id(raw_conn)
                                logger.debug(
                                    "pooled_connection_force_close",
                                    extra={"connection_id": conn_id},
                                )
                                try:
                                    closer()
                                except (RuntimeError, InternalClientError) as exc:
                                    logger.warning(
                                        "pooled_connection_force_close_failed",
                                        extra={
                                            "connection_id": conn_id,
                                            "error": str(exc),
                                        },
                                        exc_info=exc,
                                    )
                    # Clear the pool so SQLAlchemy does not retry termination.
                    try:  # queue.Queue or asyncio.Queue
                        raw_pool.queue.clear()  # type: ignore[attr-defined]
                    except Exception:
                        try:
                            raw_pool.items.clear()  # type: ignore[attr-defined]
                        except Exception:
                            try:
                                raw_pool._queue.clear()  # type: ignore[attr-defined]
                            except Exception:
                                pass
        else:
            await _engine.dispose()
    finally:
        _engine = None
        _engine_loop = None
