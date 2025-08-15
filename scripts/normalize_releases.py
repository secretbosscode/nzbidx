#!/usr/bin/env python3
"""Re-normalize existing releases and merge duplicates."""

from __future__ import annotations

import re
from typing import Any

from nzbidx_ingest.main import (
    connect_db,
    connect_opensearch,
    insert_release,
    bulk_index_releases,
)
from nzbidx_ingest.parsers import normalize_subject


_SENTINEL = object()


def normalize_releases(
    conn: Any | None = None, os_client: object | None = _SENTINEL
) -> None:
    """Normalize ``release`` rows and update storage backends."""
    if conn is None:
        conn = connect_db()
    if os_client is _SENTINEL:
        os_client = connect_opensearch()

    cur = conn.cursor()
    cur.execute(
        "SELECT norm_title, category, language, tags, source_group, size_bytes FROM release"
    )
    rows = cur.fetchall()

    aggregated: dict[str, dict[str, Any]] = {}
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    for norm_title, category, language, tags, source_group, size_bytes in rows:
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

    releases: list[
        tuple[str, str | None, str | None, list[str], str | None, int | None]
    ] = []
    docs: list[tuple[str, dict[str, object]]] = []
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
            )
        )
        body: dict[str, object] = {"norm_title": key}
        if info["category"]:
            body["category"] = info["category"]
        if info["language"]:
            body["language"] = info["language"]
        if tags_sorted:
            body["tags"] = tags_sorted
        if info["source_group"]:
            body["source_group"] = info["source_group"]
        if info["size_bytes"]:
            body["size_bytes"] = info["size_bytes"]
        docs.append((key, body))

    with conn:
        conn.execute("DELETE FROM release")
        insert_release(conn, releases=releases)

    bulk_index_releases(os_client, docs)


if __name__ == "__main__":  # pragma: no cover - manual execution
    normalize_releases()
