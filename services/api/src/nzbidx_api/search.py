"""Database search helpers using PostgreSQL full text search."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

try:  # pragma: no cover - optional dependency
    from sqlalchemy import text
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore

from .config import _int_env
from .db import get_engine
from .metrics_log import inc

logger = logging.getLogger(__name__)

MAX_LIMIT = _int_env("MAX_LIMIT", 100)
MAX_OFFSET = _int_env("MAX_OFFSET", 10_000)


def _format_pubdate(dt: datetime | str | None) -> str:
    """Return ``dt`` converted to RFC 2822 format."""

    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            dt = None
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)


async def search_releases_async(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    limit: int,
    offset: int = 0,
    sort: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Search releases using PostgreSQL full text search."""

    if limit < 0:
        raise ValueError("limit must be >= 0")
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit > MAX_LIMIT:
        raise ValueError("limit too high")
    if offset > MAX_OFFSET:
        offset = MAX_OFFSET

    conditions = ["has_parts = TRUE", "size_bytes > 0"]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if q:
        conditions.append("search_vector @@ plainto_tsquery('simple', :tsquery)")
        params["tsquery"] = q

    if category:
        cats = [c.strip() for c in category.split(",") if c.strip()]
        if cats:
            if len(cats) == 1:
                conditions.append("category = :category")
                params["category"] = cats[0]
            else:
                conditions.append("category = ANY(:categories)")
                params["categories"] = cats

    if tag:
        conditions.append("tags LIKE :tag")
        params["tag"] = f"{tag}%"

    order_map = {
        "date": "posted_at",
        "size": "size_bytes",
        "title": "norm_title",
    }
    sort_key = sort or "date"
    sort_field = order_map.get(sort_key, "posted_at")

    where_clause = " AND ".join(conditions)
    sql = text(
        f"""
        SELECT id, norm_title, category, size_bytes, posted_at
        FROM release
        WHERE {where_clause}
        ORDER BY {sort_field} DESC
        LIMIT :limit OFFSET :offset
        """
    )

    items: List[Dict[str, str]] = []
    engine = get_engine()
    if not engine or text is None:
        logger.error(
            "search_backend_unconfigured",
            extra={"engine": bool(engine), "sqlalchemy": text is not None},
        )
        raise RuntimeError("search backend unavailable")

    rows = []
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        start_time = time.monotonic()
        try:
            async with engine.connect() as conn:
                result = await conn.execute(sql, params)
                rows = result.fetchall()
            duration_ms = int((time.monotonic() - start_time) * 1000)
            inc("search_db_query_ms", value=duration_ms)
            logger.info(
                "search_query",
                extra={"duration_ms": duration_ms, "attempt": attempt},
            )
            break
        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            inc("search_db_query_fail_total")
            extra_info = {
                "error": str(exc),
                "attempt": attempt,
                "duration_ms": duration_ms,
            }
            if isinstance(exc, OSError) or getattr(
                exc, "connection_invalidated", False
            ):
                logger.warning(
                    "search_retry",
                    extra={**extra_info, "max_attempts": max_attempts},
                )
                if attempt == max_attempts:
                    return items
                await asyncio.sleep(0.1 * attempt)
                continue
            logger.warning("search_query_failed", extra=extra_info)
            raise

    skip_count = 0
    for row in rows:
        size = row.size_bytes
        if size is None or size <= 0:
            skip_count += 1
            continue
        release_id = str(row.id)
        link = f"/api?t=getnzb&id={quote(release_id, safe='')}"
        if api_key:
            link += f"&apikey={quote(api_key, safe='')}"
        items.append(
            {
                "title": row.norm_title or "",
                "guid": release_id,
                "pubDate": _format_pubdate(row.posted_at),
                "category": row.category or "",
                "link": link,
                "size": str(size),
            }
        )
    if skip_count:
        logger.info("search_invalid_size", extra={"skip_count": skip_count})
    return items


def search_releases(
    q: Optional[str],
    *,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    limit: int,
    offset: int = 0,
    sort: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Synchronous wrapper for tests."""

    return asyncio.run(
        search_releases_async(
            q,
            category=category,
            tag=tag,
            limit=limit,
            offset=offset,
            sort=sort,
            api_key=api_key,
        )
    )
