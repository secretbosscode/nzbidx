"""Database utilities for the API service.

The global async SQLAlchemy engine is bound to the event loop on which
``init_engine`` was last called.  Reusing the engine across different event
loops is unsupported and will raise a ``RuntimeError`` when ``get_engine`` is
invoked from a foreign loop.  Each loop should initialize its own engine
instance.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import pkgutil
from functools import lru_cache
from importlib import resources
from urllib.parse import urlparse, urlunparse

from typing import Any, Optional

from . import migrations as migrations_pkg

from nzbidx_ingest.db_migrations import (
    migrate_release_adult_partitions,
    drop_unused_release_adult_partitions,
)
from nzbidx_migrations import apply_async, _split_sql

# Optional SQLAlchemy dependency
try:  # pragma: no cover - import guard
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore
    AsyncEngine = None  # type: ignore
    create_async_engine = None  # type: ignore

# Optional sqlparse dependency for parsing schema statements.  When unavailable
# the internal splitter from nzbidx_migrations is used.
try:  # pragma: no cover - optional dependency
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
    """Return the loop-bound engine instance if initialized.

    The engine may only be retrieved from the event loop that initialized it.
    Calling this function from a different running loop raises ``RuntimeError``
    to avoid cross-loop reuse which can leave connections hanging.
    """

    engine = _engine
    if engine is None:
        return None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop; allow access for cleanup or synchronous callers.
        return engine
    if _engine_loop is not loop:
        raise RuntimeError(
            "database engine initialized on a different event loop; call"
            " init_engine() for this loop first"
        )
    return engine


@lru_cache()
def load_schema_statements() -> list[str]:
    sql = (
        resources.files(__package__).joinpath("schema.sql").read_text(encoding="utf-8")
    )
    if sqlparse:
        return [s.strip() for s in sqlparse.split(sql) if s.strip()]
    return _split_sql(sql)


async def apply_schema(max_attempts: int = 5, retry_delay: float = 1.0) -> None:
    """Create database schema if it does not already exist."""
    engine = get_engine()
    if not engine or not text:
        return

    statements = load_schema_statements()

    async def _apply(conn: Any) -> None:
        partitioned = False
        try:
            partitioned = bool(
                await conn.scalar(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM pg_partitioned_table "
                        "WHERE partrelid = to_regclass('release_adult')"
                        ")"
                    )
                )
            )
            if not partitioned:
                try:
                    raw = engine.sync_engine.raw_connection()
                    try:
                        migrate_release_adult_partitions(raw)
                    finally:  # pragma: no cover - connection cleanup
                        try:
                            raw.close()
                        except DB_CLOSE_ERRORS:
                            pass
                    partitioned = bool(
                        await conn.scalar(
                            text(
                                "SELECT EXISTS ("
                                "SELECT 1 FROM pg_partitioned_table "
                                "WHERE partrelid = to_regclass('release_adult')"
                                ")"
                            )
                        )
                    )
                except Exception as exc:  # pragma: no cover - best effort
                    logger.warning(
                        "release_adult_migration_failed",
                        exc_info=True,
                        extra={"error": str(exc)},
                    )
        except Exception:  # pragma: no cover - system catalogs missing
            partitioned = False

        try:
            raw = engine.sync_engine.raw_connection()  # type: ignore[attr-defined]
            try:
                drop_unused_release_adult_partitions(raw)
            finally:  # pragma: no cover - connection cleanup
                try:
                    raw.close()
                except DB_CLOSE_ERRORS:
                    pass
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning(
                "release_adult_partition_cleanup_failed",
                exc_info=True,
                extra={"error": str(exc)},
            )

        def _predicate(stmt: str) -> bool:
            if not partitioned and "PARTITION OF release_adult" in stmt:
                raise RuntimeError(f"release_adult_unpartitioned: {stmt}")
            return True

        await apply_async(conn, text, statements=statements, predicate=_predicate)

    async def _run_migrations(conn: Any) -> None:
        """Import and execute database migrations."""

        def _migrate(sync_conn: Any) -> None:
            raw = sync_conn.connection.dbapi_connection
            cur = raw.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS nzbidx_schema_migrations (name TEXT PRIMARY KEY)"
            )
            try:
                raw.commit()
            except Exception:
                pass
            modules = sorted(
                pkgutil.iter_modules(migrations_pkg.__path__), key=lambda m: m.name
            )
            for info in modules:
                name = info.name
                cur.execute(
                    f"SELECT 1 FROM nzbidx_schema_migrations WHERE name = '{name}'"
                )
                if cur.fetchone():
                    continue
                module = importlib.import_module(f"{migrations_pkg.__name__}.{name}")
                migrate = getattr(module, "migrate", None)
                if migrate:
                    migrate(raw)
                    cur.execute(
                        f"INSERT INTO nzbidx_schema_migrations (name) VALUES ('{name}')"
                    )
                    try:
                        raw.commit()
                    except Exception:
                        pass

        if not hasattr(conn, "run_sync") or not hasattr(conn, "execution_options"):
            return
        try:
            autocommit_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await autocommit_conn.run_sync(_migrate)
        except Exception as exc:
            logger.error("migration_failed", exc_info=True, extra={"error": str(exc)})
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
                await _run_migrations(conn)
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
                    await conn.execute(text("CREATE DATABASE :name"), {"name": dbname})
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


@lru_cache(maxsize=None)
def _sql_placeholder_by_type(conn_cls: type[Any]) -> str:
    """Return the DB-API parameter placeholder for the given connection class."""
    return "?" if conn_cls.__module__.startswith("sqlite3") else "%s"


def sql_placeholder(conn: Any) -> str:
    """Return the DB-API parameter placeholder for ``conn``.

    Results are cached per connection class so callers may continue to pass
    connection instances without incurring repeat inspection overhead.
    """
    return _sql_placeholder_by_type(type(conn))


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
                                "terminate",
                                getattr(proto, "close_transport", None),
                            )
                            if callable(closer):
                                try:
                                    fut = closer()
                                    if asyncio.iscoroutine(fut):
                                        try:
                                            await fut
                                        except InternalClientError as exc:
                                            logger.warning(
                                                "pooled_connection_force_close_failed",
                                                extra={
                                                    "connection_id": id(raw_conn),
                                                    "error": str(exc),
                                                },
                                            )
                                        except Exception:
                                            pass
                                    elif asyncio.isfuture(fut):
                                        if not fut.done():
                                            fut.cancel()
                                        try:
                                            fut.exception()
                                        except InternalClientError as exc:
                                            logger.warning(
                                                "pooled_connection_force_close_failed",
                                                extra={
                                                    "connection_id": id(raw_conn),
                                                    "error": str(exc),
                                                },
                                            )
                                        except Exception:
                                            pass
                                except InternalClientError as exc:
                                    logger.warning(
                                        "pooled_connection_force_close_failed",
                                        extra={
                                            "connection_id": id(raw_conn),
                                            "error": str(exc),
                                        },
                                    )
                                except RuntimeError:
                                    pass
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
