"""Header-only ingest loop."""

from __future__ import annotations

import logging
import time
from threading import Event

from .config import (
    INGEST_BATCH_MIN,
    INGEST_BATCH_MAX,
    INGEST_POLL_MIN_SECONDS,
    INGEST_POLL_MAX_SECONDS,
    INGEST_OS_LATENCY_MS,
    CB_RESET_SECONDS,
    INGEST_SLEEP_MS,
    INGEST_DB_LATENCY_MS,
)
from . import config, cursors
from .nntp_client import NNTPClient
from .parsers import normalize_subject, detect_language
from .main import (
    insert_release,
    bulk_index_releases,
    index_release,  # noqa: F401  # backward compat for tests
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

# Track consecutive failures per group to allow backoff or alerting.
# This is reset on successful xover calls.
_group_failures: dict[str, int] = {}

# Counter used to throttle how often batch metrics are logged at INFO level.
_log_counter = 0


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
            "eta_seconds": 0,
        }
        if self._duration_s > 0 and self._processed > 0 and self._remaining > 0:
            rate = self._processed / self._duration_s
            summary["eta_seconds"] = int(self._remaining / rate)
        summary["eta_s"] = summary["eta_seconds"]
        return summary


def run_once() -> float:
    """Process a single batch for each configured NNTP group.

    Returns the suggested delay before the next poll.
    """
    groups = config.NNTP_GROUPS or config._load_groups()
    ignored = set(config.IGNORE_GROUPS or [])
    if ignored:
        logger.info("ingest_ignore_groups", extra={"groups": list(ignored)})
    groups = [g for g in groups if g not in ignored]
    if not groups:
        logger.info("ingest_no_groups")
        return INGEST_POLL_MAX_SECONDS
    skip = set(cursors.get_irrelevant_groups())
    if skip:
        groups = [g for g in groups if g not in skip]
    if not groups:
        logger.info("ingest_no_groups")
        return INGEST_POLL_MAX_SECONDS
    config.NNTP_GROUPS = groups
    logger.info("ingest_groups", extra={"count": len(groups), "groups": groups})

    client = NNTPClient()
    client.connect()
    db = connect_db()
    os_client = connect_opensearch()

    aggregate = _AggregateMetrics()

    for ig in ignored:
        prune_group(db, os_client, ig)

    for group in groups:
        last = cursors.get_cursor(group) or 0
        start = last + 1
        high = client.high_water_mark(group)
        remaining = max(high - last, 0)
        if remaining <= 0:
            headers: list[dict[str, object]] = []
        else:
            batch = min(remaining, INGEST_BATCH_MAX)
            batch = max(batch, min(remaining, INGEST_BATCH_MIN))
            end = start + batch - 1
            try:
                headers = client.xover(group, start, end)
                _group_failures[group] = 0
            except Exception:
                failures = _group_failures.get(group, 0) + 1
                _group_failures[group] = failures
                logger.exception(
                    "ingest_xover_error",
                    extra={
                        "group": group,
                        "start": start,
                        "end": end,
                        "failures": failures,
                    },
                )
                if failures >= 3:
                    logger.warning(
                        "ingest_xover_consecutive_failures",
                        extra={"group": group, "failures": failures},
                    )
                continue
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
        batch_start = time.monotonic()
        current = last
        releases: dict[
            str,
            tuple[
                str,
                str | None,
                str | None,
                list[str] | None,
                str | None,
                int | None,
            ],
        ] = {}
        docs: dict[str, dict[str, object]] = {}
        for idx, header in enumerate(headers, start=start):
            metrics["processed"] += 1
            size = int(header.get("bytes") or header.get(":bytes") or 0)
            current = idx
            if size <= 0:
                continue
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
            existing = releases.get(dedupe_key)
            if existing:
                _, ex_cat, ex_lang, ex_tags, ex_group, ex_size = existing
                combined_size = (ex_size or 0) + size
                combined_tags = sorted(set(ex_tags or []).union(tags))
                releases[dedupe_key] = (
                    dedupe_key,
                    ex_cat,
                    ex_lang,
                    combined_tags,
                    ex_group,
                    combined_size,
                )
                body = docs.get(dedupe_key, {"norm_title": dedupe_key})
                if combined_tags:
                    body["tags"] = combined_tags
                if combined_size > 0:
                    body["size_bytes"] = combined_size
                docs[dedupe_key] = body
            else:
                releases[dedupe_key] = (
                    dedupe_key,
                    category,
                    language,
                    tags,
                    group,
                    size,
                )
                body: dict[str, object] = {"norm_title": dedupe_key}
                if category:
                    body["category"] = category
                if language:
                    body["language"] = language
                if tags:
                    body["tags"] = tags
                if group:
                    body["source_group"] = group
                if size > 0:
                    body["size_bytes"] = size
                docs[dedupe_key] = body
        db_latency = 0.0
        os_latency = 0.0
        inserted: set[str] = set()
        if releases:
            db_start = time.monotonic()
            result = insert_release(db, releases=releases.values())
            db_latency = time.monotonic() - db_start
            if isinstance(result, set):
                inserted = result
            elif result:
                inserted = {r[0] for r in releases.values()}
            metrics["inserted"] = len(inserted)
        to_index = [(doc_id, docs[doc_id]) for doc_id in inserted if doc_id in docs]
        if to_index:
            os_start = time.monotonic()
            bulk_index_releases(os_client, to_index)
            os_latency = time.monotonic() - os_start
            metrics["indexed"] = len(to_index)
        cursors.set_cursor(group, current)
        metrics["deduplicated"] = metrics["processed"] - metrics["inserted"]
        metrics["deduped"] = metrics["deduplicated"]
        duration_s = time.monotonic() - batch_start
        metrics["duration_ms"] = int(duration_s * 1000)
        metrics["average_batch_ms"] = (
            round((duration_s * 1000) / metrics["processed"], 3)
            if metrics["processed"]
            else 0.0
        )
        metrics["avg_batch_ms"] = metrics["average_batch_ms"]
        metrics["opensearch_latency_ms"] = int(os_latency * 1000)
        metrics["os_latency_ms"] = metrics["opensearch_latency_ms"]
        avg_db_ms = (
            round((db_latency * 1000) / metrics["processed"], 3)
            if metrics["processed"]
            else 0.0
        )
        avg_os_ms = (
            round((os_latency * 1000) / metrics["indexed"], 3)
            if metrics["indexed"]
            else 0.0
        )
        metrics["average_database_latency_ms"] = avg_db_ms
        metrics["average_opensearch_latency_ms"] = avg_os_ms
        metrics["avg_db_ms"] = avg_db_ms
        metrics["avg_os_ms"] = avg_os_ms
        metrics["cursor"] = current
        metrics["high_water"] = high
        remaining = max(high - current, 0)
        metrics["remaining"] = remaining
        if high > 0:
            metrics["percent_complete"] = int(current / high * 100)
            metrics["pct_complete"] = metrics["percent_complete"]
        if duration_s > 0 and metrics["processed"] > 0 and remaining > 0:
            rate = metrics["processed"] / duration_s
            metrics["eta_seconds"] = int(remaining / rate)
            metrics["eta_s"] = metrics["eta_seconds"]
        metrics["group"] = group
        global _log_counter
        _log_counter += 1
        log_fn = logger.debug
        if metrics["inserted"] > 0 or (
            config.INGEST_LOG_EVERY > 0 and _log_counter % config.INGEST_LOG_EVERY == 0
        ):
            log_fn = logger.info
        log_fn("ingest_batch", extra=metrics)
        aggregate.add(metrics)
        if metrics["inserted"] == 0:
            cursors.mark_irrelevant(group)
        sleep_ms = 0
        if os_breaker.is_open():
            sleep_ms = max(sleep_ms, int(CB_RESET_SECONDS * 500))
        if INGEST_SLEEP_MS > 0 and (
            avg_db_ms > INGEST_DB_LATENCY_MS or avg_os_ms > INGEST_OS_LATENCY_MS
        ):
            ratio = 1.0
            if avg_db_ms > INGEST_DB_LATENCY_MS and INGEST_DB_LATENCY_MS > 0:
                ratio = max(ratio, avg_db_ms / INGEST_DB_LATENCY_MS)
            if avg_os_ms > INGEST_OS_LATENCY_MS and INGEST_OS_LATENCY_MS > 0:
                ratio = max(ratio, avg_os_ms / INGEST_OS_LATENCY_MS)
            sleep_ms = max(sleep_ms, int(INGEST_SLEEP_MS * ratio))
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000)

    summary = aggregate.summary()
    logger.info("ingest_summary", extra={"event": "ingest_summary", **summary})
    remaining = summary.get("remaining", 0)
    processed = summary.get("processed", 0)
    if remaining <= 0:
        return INGEST_POLL_MAX_SECONDS
    if processed <= 0:
        return INGEST_POLL_MIN_SECONDS
    ratio = remaining / (processed + remaining)
    delay = INGEST_POLL_MIN_SECONDS + (
        INGEST_POLL_MAX_SECONDS - INGEST_POLL_MIN_SECONDS
    ) * (1 - ratio)
    return delay


def run_forever(stop_event: Event | None = None) -> None:
    """Continuously poll groups until ``stop_event`` is set."""
    while not (stop_event and stop_event.is_set()):
        try:
            delay = run_once()
        except Exception:
            logger.exception("ingest_loop_failure")
            delay = INGEST_POLL_MAX_SECONDS
        delay = max(INGEST_POLL_MIN_SECONDS, min(INGEST_POLL_MAX_SECONDS, delay))
        if stop_event:
            if stop_event.wait(delay):
                break
        else:
            time.sleep(delay)
