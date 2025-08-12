"""Database utilities for the API service."""

from __future__ import annotations

import logging
import os
from pathlib import Path

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
    schema_path = Path(__file__).resolve().parent.parent / "db" / "init" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    async with engine.begin() as conn:
        for stmt in statements:
            try:
                await conn.execute(text(stmt))
            except Exception as exc:
                # Creating extensions requires superuser privileges.  If the
                # current role lacks permission, log the failure but continue
                # applying the remaining schema.
                if stmt.lstrip().upper().startswith("CREATE EXTENSION"):
                    logger.warning(
                        "extension_unavailable", extra={"stmt": stmt, "error": str(exc)}
                    )
                else:
                    raise


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
