"""Header-only ingest loop."""

from __future__ import annotations

import logging
import time
from threading import Event

from .config import (
    INGEST_BATCH,
    INGEST_POLL_SECONDS,
    INGEST_OS_LATENCY_MS,
    CB_RESET_SECONDS,
    INGEST_OS_BULK,
)
from . import config, cursors
from .nntp_client import NNTPClient
from .parsers import normalize_subject, detect_language
from .main import (
    insert_release,
    bulk_index_releases,
    index_release,  # noqa: F401 - retained for backward compatibility
    _infer_category,
    connect_db,
    connect_opensearch,
    CATEGORY_MAP,
    prune_group,
)

try:  # pragma: no cover - optional import
    from nzbidx_api.middleware_circuit import os_breaker  # type: ignore
except Exception:  # pragma: no cover - fallback when api pkg missing

    class _DummyBreaker:
        def is_open(self) -> bool:  # pragma: no cover - trivial
            return False

    os_breaker = _DummyBreaker()
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


class _AggregateMetrics:
    """Helper to accumulate metrics across groups during a poll cycle."""

    def __init__(self) -> None:
        self._processed = 0
        self._remaining = 0
        self._duration_s = 0.0

    def add(self, metrics: dict[str, int | float]) -> None:
        """Add per-group metrics to the aggregate."""
        self._processed += int(metrics.get("processed", 0))
        self._remaining += int(metrics.get("remaining", 0))
        self._duration_s += float(metrics.get("duration_ms", 0)) / 1000

    def summary(self) -> dict[str, int]:
        """Return aggregate metrics including global ETA."""
        summary: dict[str, int] = {
            "processed": self._processed,
            "remaining": self._remaining,
            "eta_s": 0,
        }
        if self._duration_s > 0 and self._processed > 0 and self._remaining > 0:
            rate = self._processed / self._duration_s
            summary["eta_s"] = int(self._remaining / rate)
        return summary


def run_once() -> None:
    """Process a single batch for each configured NNTP group."""
    groups = config.NNTP_GROUPS or config._load_groups()
    ignored = set(config.IGNORE_GROUPS or [])
    if ignored:
        logger.info("ingest_ignore_groups", extra={"groups": list(ignored)})
    groups = [g for g in groups if g not in ignored]
    if not groups:
        logger.info("ingest_no_groups")
        return
    skip = set(cursors.get_irrelevant_groups())
    if skip:
        groups = [g for g in groups if g not in skip]
    if not groups:
        logger.info("ingest_no_groups")
        return
    config.NNTP_GROUPS = groups
    logger.info("ingest_groups", extra={"count": len(groups), "groups": groups})

    client = NNTPClient()
    client.connect()
    db = connect_db()
    os_client = connect_opensearch()
    bulk_docs: list[tuple[str, dict[str, object]]] = []

    aggregate = _AggregateMetrics()

    for ig in ignored:
        prune_group(db, os_client, ig)

    for group in groups:
        last = cursors.get_cursor(group) or 0
        start = last + 1
        end = start + INGEST_BATCH - 1
        high = client.high_water_mark(group)
        headers = client.xover(group, start, end)
        if not headers:
            logger.info(
                "ingest_idle",
                extra={"group": group, "cursor": last, "high_water": high},
            )
            # ``high`` is ``0`` when the NNTP server is unreachable.  Avoid
            # marking the group as irrelevant in that case so it will be
            # retried once connectivity is restored.
            if high > 0:
                cursors.mark_irrelevant(group)
            continue
        metrics = {"processed": 0, "inserted": 0, "indexed": 0}
        last_os_latency = 0.0
        batch_start = time.monotonic()
        current = last
        for idx, header in enumerate(headers, start=start):
            metrics["processed"] += 1
            subject = header.get("subject", "")
            norm_title, tags = normalize_subject(subject, with_tags=True)
            norm_title = norm_title.lower()
            posted = header.get("date")
            day_bucket = ""
            if posted:
                try:
                    day_bucket = parsedate_to_datetime(str(posted)).strftime("%Y-%m-%d")
                except Exception:
                    day_bucket = ""
            dedupe_key = f"{norm_title}:{day_bucket}" if day_bucket else norm_title
            language = detect_language(subject) or "und"
            category = _infer_category(subject, group) or CATEGORY_MAP["other"]
            tags = tags or []
            start_idx = time.monotonic()
            inserted = insert_release(
                db,
                dedupe_key,
                category,
                language,
                tags,
                group,
            )
            if inserted:
                metrics["inserted"] += 1
                body: dict[str, object] = {"norm_title": dedupe_key}
                if category:
                    body["category"] = category
                if language:
                    body["language"] = language
                if tags:
                    body["tags"] = tags
                if group:
                    body["source_group"] = group
                bulk_docs.append((dedupe_key, body))
                metrics["indexed"] += 1
                if len(bulk_docs) >= INGEST_OS_BULK:
                    os_start = time.monotonic()
                    bulk_index_releases(os_client, bulk_docs)
                    last_os_latency = time.monotonic() - os_start
                    bulk_docs.clear()
            latency = time.monotonic() - start_idx
            if os_breaker.is_open():
                time.sleep(CB_RESET_SECONDS / 2)
            elif last_os_latency * 1000 > INGEST_OS_LATENCY_MS:
                time.sleep(min(last_os_latency / 2, 2))
            elif latency > 0.5:
                time.sleep(min(latency, 5))
            current = idx
        if bulk_docs:
            os_start = time.monotonic()
            bulk_index_releases(os_client, bulk_docs)
            last_os_latency = time.monotonic() - os_start
            bulk_docs.clear()
        cursors.set_cursor(group, current)
        metrics["deduped"] = metrics["processed"] - metrics["inserted"]
        duration_s = time.monotonic() - batch_start
        metrics["duration_ms"] = int(duration_s * 1000)
        metrics["os_latency_ms"] = int(last_os_latency * 1000)
        metrics["cursor"] = current
        metrics["high_water"] = high
        remaining = max(high - current, 0)
        metrics["remaining"] = remaining
        if high > 0:
            metrics["pct_complete"] = int(current / high * 100)
        if duration_s > 0 and metrics["processed"] > 0 and remaining > 0:
            rate = metrics["processed"] / duration_s
            metrics["eta_s"] = int(remaining / rate)
        metrics["group"] = group
        logger.info("ingest_batch", extra=metrics)
        aggregate.add(metrics)
        if metrics["inserted"] == 0:
            cursors.mark_irrelevant(group)

    logger.info("ingest_summary", extra=aggregate.summary())


def run_forever(stop_event: Event | None = None) -> None:
    """Continuously poll groups until ``stop_event`` is set."""
    while not (stop_event and stop_event.is_set()):
        run_once()
        if stop_event:
            if stop_event.wait(INGEST_POLL_SECONDS):
                break
        else:
            time.sleep(INGEST_POLL_SECONDS)
