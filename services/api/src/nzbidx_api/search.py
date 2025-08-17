"""Database search helpers using PostgreSQL full text search."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

try:  # pragma: no cover - optional dependency
    from sqlalchemy import text
except Exception:  # pragma: no cover - optional dependency
    text = None  # type: ignore

from .config import _int_env
from .db import engine
from .newznab import ADULT_CATEGORY_ID, adult_content_allowed

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

    conditions = ["has_parts = TRUE"]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if q:
        tokens = [t for t in re.split(r"\s+", q) if t]
        tsquery = " & ".join(tokens)
        conditions.append(
            "to_tsvector('simple', coalesce(norm_title, '') || ' ' || coalesce(tags, '')) @@ to_tsquery('simple', :tsquery)"
        )
        params["tsquery"] = tsquery

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

    if not adult_content_allowed():
        conditions.append("left(category, 1) != :adult")
        params["adult"] = str(ADULT_CATEGORY_ID)[0]

    order_map = {
        "date": "posted_at",
        "size": "size_bytes",
        "title": "norm_title",
    }
    sort_field = order_map.get(sort or "date", sort or "posted_at")

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
    if not engine or text is None:
        return items

    async with engine.connect() as conn:
        result = await conn.execute(sql, params)
        rows = result.fetchall()

    for row in rows:
        size = row.size_bytes
        if size is None or size <= 0:
            continue
        release_id = str(row.id)
        link = f"/api?t=getnzb&id={quote(release_id, safe='')}"
        if api_key:
            link += f"&apikey={api_key}"
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
