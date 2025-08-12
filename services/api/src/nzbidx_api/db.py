"""Database utilities for the API service."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from typing import Any, Dict, List, Optional, Sequence

# Optional SQLAlchemy dependency
try:  # pragma: no cover - import guard
    from sqlalchemy import create_engine, text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore
    create_engine = None  # type: ignore
    AsyncEngine = None  # type: ignore
    create_async_engine = None  # type: ignore

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
    schema_path = Path(__file__).resolve().parent.parent / "db" / "init" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    try:
        async with engine.begin() as conn:
            for stmt in statements:
                await conn.execute(text(stmt))
    except Exception as exc:  # pragma: no cover - network errors
        msg = str(getattr(exc, "orig", exc)).lower()
        if "does not exist" not in msg and "invalid catalog name" not in msg:
            raise
        parsed = urlparse(DATABASE_URL)
        dbname = parsed.path.lstrip("/")
        admin_url = urlunparse(
            parsed._replace(path="/postgres", scheme="postgresql+psycopg")
        )
        if not create_engine:
            raise
        admin_engine = create_engine(admin_url, echo=False, future=True)
        with admin_engine.begin() as conn:  # type: ignore[call-arg]
            conn.execute(text(f'CREATE DATABASE "{dbname}"'))
        admin_engine.dispose()
        async with engine.begin() as conn:
            for stmt in statements:
                await conn.execute(text(stmt))


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
