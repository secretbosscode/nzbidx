"""Database utilities for the API service."""

from __future__ import annotations

import logging
import os
from importlib import resources
from urllib.parse import urlparse, urlunparse

from typing import Any, Dict, List, Optional, Sequence

# Optional SQLAlchemy dependency
try:  # pragma: no cover - import guard
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore
    AsyncEngine = None  # type: ignore
    create_async_engine = None  # type: ignore

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://localhost:5432/postgres")
DATABASE_URL = os.path.expandvars(DATABASE_URL)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

if create_async_engine:
    engine: Optional[AsyncEngine] = create_async_engine(DATABASE_URL, echo=False)
else:  # pragma: no cover - no sqlalchemy available
    engine = None


async def apply_schema() -> None:
    """Create database schema if it does not already exist."""
    if not engine or not text:
        return
    sql = (
        resources.files(__package__).joinpath("schema.sql").read_text(encoding="utf-8")
    )
    statements = [s.strip() for s in sql.split(";") if s.strip()]

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

    try:
        async with engine.connect() as conn:
            await _apply(conn)
    except Exception as exc:
        msg = str(getattr(exc, "orig", exc)).lower()
        if "does not exist" not in msg and "invalid catalog name" not in msg:
            raise
        await _create_database(DATABASE_URL)
        async with engine.connect() as conn:
            await _apply(conn)


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


async def similar_releases(
    embedding: Sequence[float], *, limit: int = 10
) -> List[Dict[str, Any]]:
    """Return releases ordered by vector distance.

    Utilises the ``pgvector`` ivfflat index to efficiently search for
    nearest neighbours based on the supplied ``embedding``.
    """
    if not engine or not text:
        return []
    stmt = text(
        "SELECT id, title, category, language FROM release "
        "ORDER BY embedding <-> :embedding LIMIT :limit"
    )
    async with engine.connect() as conn:
        result = await conn.execute(
            stmt, {"embedding": list(embedding), "limit": limit}
        )
        return [dict(row) for row in result]
