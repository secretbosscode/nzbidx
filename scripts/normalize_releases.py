#!/usr/bin/env python3
"""Re-normalize existing releases and merge duplicates."""

from __future__ import annotations

import re
from typing import Any

from nzbidx_ingest.main import (
    connect_db,
    insert_release,
)
from nzbidx_ingest.parsers import normalize_subject


def normalize_releases(
    conn: Any | None = None,
) -> None:
    """Normalize ``release`` rows and update storage backends."""
    if conn is None:
        conn = connect_db()

    cur = conn.cursor()
    cur.execute(
        "SELECT norm_title, category, language, tags, source_group, size_bytes, posted_at FROM release"
    )
    rows = cur.fetchall()

    aggregated: dict[str, dict[str, Any]] = {}
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    for (
        norm_title,
        category,
        language,
        tags,
        source_group,
        size_bytes,
        posted_at,
    ) in rows:
        title = norm_title or ""
        date: str | None = None
        if ":" in title:
            maybe_title, maybe_date = title.rsplit(":", 1)
            if date_re.fullmatch(maybe_date):
                title = maybe_title
                date = maybe_date
        normalized = normalize_subject(title)
        key = f"{normalized}:{date}" if date else normalized
        tag_list = [t for t in (tags or "").split(",") if t]

        agg = aggregated.get(key)
        if not agg:
            aggregated[key] = {
                "category": category,
                "language": language,
                "tags": set(tag_list),
                "source_group": source_group,
                "size_bytes": int(size_bytes or 0),
                "posted_at": posted_at,
            }
            continue
        agg["size_bytes"] += int(size_bytes or 0)
        agg["tags"].update(tag_list)
        if not agg["category"] and category:
            agg["category"] = category
        if not agg["language"] and language:
            agg["language"] = language
        if not agg["source_group"] and source_group:
            agg["source_group"] = source_group
        if posted_at:
            cur_posted = agg.get("posted_at")
            if not cur_posted or posted_at < cur_posted:
                agg["posted_at"] = posted_at

    releases: list[
        tuple[
            str, str | None, str | None, list[str], str | None, int | None, str | None
        ]
    ] = []
    for key, info in aggregated.items():
        tags_sorted = sorted(info["tags"])
        releases.append(
            (
                key,
                info["category"],
                info["language"],
                tags_sorted,
                info["source_group"],
                info["size_bytes"],
                info.get("posted_at"),
            )
        )

    with conn:
        conn.execute("DELETE FROM release")
        insert_release(conn, releases=releases)


if __name__ == "__main__":  # pragma: no cover - manual execution
    normalize_releases()
