"""Header-only ingest loop."""

from __future__ import annotations

import json
import logging
import time
from threading import Event

from .config import (
    INGEST_BATCH_MIN,
    INGEST_BATCH_MAX,
    INGEST_POLL_MIN_SECONDS,
    INGEST_POLL_MAX_SECONDS,
    INGEST_SLEEP_MS,
    INGEST_DB_LATENCY_MS,
)
from . import config, cursors
from .nntp_client import NNTPClient
from .parsers import normalize_subject, detect_language, extract_segment_number
from .segment_schema import validate_segment_schema
from .main import (
    insert_release,
    _infer_category,
    connect_db,
    CATEGORY_MAP,
    prune_group,
)
from email.utils import parsedate_to_datetime
from datetime import timezone

logger = logging.getLogger(__name__)

# Track consecutive failures per group to allow backoff or alerting.
# This is reset on successful xover calls.
_group_failures: dict[str, int] = {}

# Counter used to throttle how often batch metrics are logged at INFO level.
_log_counter = 0

# Timestamp of the last successful ingest iteration (seconds since epoch).
last_run: float = 0.0


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
        return summary


def _process_groups(
    client: NNTPClient,
    db: object,
    groups: list[str],
    ignored: set[str],
) -> float:
    aggregate = _AggregateMetrics()

    for ig in ignored:
        prune_group(db, ig)

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
        metrics = {"processed": 0, "inserted": 0}
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
                str | None,
            ],
        ] = {}
        parts: dict[str, list[tuple[int, str, str, int]]] = {}
        for idx, header in enumerate(headers, start=start):
            metrics["processed"] += 1
            size = int(header.get("bytes") or header.get(":bytes") or 0)
            current = idx
            message_id = str(header.get("message-id") or "").strip()
            if size <= 0 and message_id:
                size = client.body_size(message_id)
            if size <= 0:
                continue
            subject = header.get("subject", "")
            norm_title, tags = normalize_subject(subject, with_tags=True)
            norm_title = norm_title.lower()
            posted = header.get("date")
            day_bucket = ""
            posted_at = None
            if posted:
                try:
                    dt = parsedate_to_datetime(str(posted)).astimezone(timezone.utc)
                    posted_at = dt.isoformat()
                    day_bucket = dt.strftime("%Y-%m-%d")
                except Exception:
                    day_bucket = ""
            dedupe_key = f"{norm_title}:{day_bucket}" if day_bucket else norm_title
            language = detect_language(subject) or "und"
            category = _infer_category(subject, group) or CATEGORY_MAP["other"]
            tags = tags or []
            existing = releases.get(dedupe_key)
            if existing:
                _, ex_cat, ex_lang, ex_tags, ex_group, ex_size, ex_posted = existing
                combined_size = (ex_size or 0) + size
                combined_tags = sorted(set(ex_tags or []).union(tags))
                combined_posted = ex_posted
                if posted_at and (not ex_posted or posted_at < ex_posted):
                    combined_posted = posted_at
                releases[dedupe_key] = (
                    dedupe_key,
                    ex_cat,
                    ex_lang,
                    combined_tags,
                    ex_group,
                    combined_size,
                    combined_posted,
                )
            else:
                releases[dedupe_key] = (
                    dedupe_key,
                    category,
                    language,
                    tags,
                    group,
                    size,
                    posted_at,
                )
            if message_id:
                seg_num = extract_segment_number(subject)
                parts.setdefault(dedupe_key, []).append(
                    (seg_num, message_id.strip("<>"), group, size)
                )
        db_latency = 0.0
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

        changed: set[str] = set()
        part_counts: dict[str, int] = {}
        has_parts_flags: dict[str, bool] = {}
        if db is not None:
            try:
                cur = db.cursor()
                placeholder = (
                    "?" if db.__class__.__module__.startswith("sqlite3") else "%s"
                )
                for title, segs in parts.items():
                    if not segs:
                        continue
                    cur.execute(
                        f"SELECT segments FROM release WHERE norm_title = {placeholder}",
                        (title,),
                    )
                    row = cur.fetchone()
                    existing_segments = []
                    if row:
                        try:
                            existing_segments = json.loads(row[0] or "[]")
                        except Exception:
                            existing_segments = []
                    validate_segment_schema(existing_segments)

                    # Deduplicate newly fetched segments by message-id before merging.
                    deduped: list[dict[str, int | str]] = []
                    seen_ids: set[str] = set()
                    for n, m, g, s in segs:
                        if m in seen_ids:
                            continue
                        seen_ids.add(m)
                        deduped.append(
                            {"number": n, "message_id": m, "group": g, "size": s}
                        )

                    existing_ids = {seg["message_id"] for seg in existing_segments}
                    new_segments = [
                        seg for seg in deduped if seg["message_id"] not in existing_ids
                    ]
                    combined_segments = existing_segments + new_segments
                    validate_segment_schema(combined_segments)
                    total_size = sum(seg["size"] for seg in combined_segments)
                    part_counts[title] = len(combined_segments)
                    has_parts = bool(combined_segments)
                    cur.execute(
                        f"UPDATE release SET segments = {placeholder}, has_parts = {placeholder}, part_count = {placeholder}, size_bytes = {placeholder} WHERE norm_title = {placeholder}",
                        (
                            json.dumps(combined_segments),
                            has_parts,
                            part_counts[title],
                            total_size,
                            title,
                        ),
                    )
                    has_parts_flags[title] = has_parts
                    changed.add(title)
                db.commit()
            except Exception:
                pass

        changed |= inserted
        cursors.set_cursor(group, current)
        metrics["deduplicated"] = metrics["processed"] - metrics["inserted"]
        duration_s = time.monotonic() - batch_start
        metrics["duration_ms"] = int(duration_s * 1000)
        metrics["average_batch_ms"] = (
            round((duration_s * 1000) / metrics["processed"], 3)
            if metrics["processed"]
            else 0.0
        )
        avg_db_ms = (
            round((db_latency * 1000) / metrics["processed"], 3)
            if metrics["processed"]
            else 0.0
        )
        metrics["average_database_latency_ms"] = avg_db_ms
        metrics["cursor"] = current
        metrics["high_water"] = high
        remaining = max(high - current, 0)
        metrics["remaining"] = remaining
        if high > 0:
            metrics["percent_complete"] = int(current / high * 100)
        if duration_s > 0 and metrics["processed"] > 0 and remaining > 0:
            rate = metrics["processed"] / duration_s
            metrics["eta_seconds"] = int(remaining / rate)
        metrics["group"] = group
        global _log_counter
        _log_counter += 1
        log_fn = logger.debug
        if metrics["inserted"] > 0 or (
            config.INGEST_LOG_EVERY > 0 and _log_counter % config.INGEST_LOG_EVERY == 0
        ):
            log_fn = logger.info
        processed = metrics["processed"]
        inserted = metrics["inserted"]
        deduplicated = metrics["deduplicated"]
        percent_complete = metrics.get("percent_complete", 0)
        eta_seconds = metrics.get("eta_seconds", 0)
        log_fn(
            f"Processed {processed} items (inserted {inserted}, deduplicated {deduplicated}). "
            f"{percent_complete}% complete, ETA {eta_seconds}s for {group}",
            extra=metrics,
        )
        aggregate.add(metrics)
        if metrics["inserted"] == 0:
            cursors.mark_irrelevant(group)
        sleep_ms = 0
        if INGEST_SLEEP_MS > 0 and avg_db_ms > INGEST_DB_LATENCY_MS:
            ratio = 1.0
            if INGEST_DB_LATENCY_MS > 0:
                ratio = max(ratio, avg_db_ms / INGEST_DB_LATENCY_MS)
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


def run_once(db: object | None) -> float:
    """Process a single batch for each configured NNTP group.

    ``db`` is an open database connection that will remain open across
    iterations.  The caller is responsible for reconnecting if the
    connection becomes unusable.

    Returns the suggested delay before the next poll.
    """
    global last_run
    groups = config.NNTP_GROUPS or config._load_groups()
    ignored = set(config.IGNORE_GROUPS or [])
    if ignored:
        logger.info("ingest_ignore_groups", extra={"groups": list(ignored)})
    groups = [g for g in groups if g not in ignored]
    if not groups:
        logger.info("ingest_no_groups")
        last_run = time.time()
        return INGEST_POLL_MAX_SECONDS
    skip = set(cursors.get_irrelevant_groups())
    if skip:
        groups = [g for g in groups if g not in skip]
    if not groups:
        logger.info("ingest_no_groups")
        last_run = time.time()
        return INGEST_POLL_MAX_SECONDS
    config.NNTP_GROUPS = groups
    logger.info("ingest_groups", extra={"count": len(groups), "groups": groups})

    client = NNTPClient()
    try:
        client.connect()
        delay = _process_groups(client, db, groups, ignored)
        last_run = time.time()
        return delay
    finally:
        try:
            client.quit()
        except Exception:
            pass


def run_forever(stop_event: Event | None = None) -> None:
    """Continuously poll groups until ``stop_event`` is set."""
    failure_delay = INGEST_POLL_MIN_SECONDS
    db: object | None = None
    try:
        while not (stop_event and stop_event.is_set()):
            try:
                if db is None:
                    db = connect_db()
                delay = run_once(db)
                failure_delay = INGEST_POLL_MIN_SECONDS
            except BaseException as exc:  # pragma: no cover
                if isinstance(exc, KeyboardInterrupt):
                    logger.info("ingest_loop_interrupted")
                    raise
                logger.exception("ingest_loop_failure")
                try:
                    if db is not None:
                        db.close()
                except Exception:
                    pass
                db = None
                delay = failure_delay
                failure_delay = min(INGEST_POLL_MAX_SECONDS, failure_delay * 2)
            delay = max(INGEST_POLL_MIN_SECONDS, min(INGEST_POLL_MAX_SECONDS, delay))
            if stop_event:
                if stop_event.wait(delay):
                    break
            else:
                try:
                    time.sleep(delay)
                except ValueError:
                    logger.exception("ingest_sleep_failure", extra={"delay": delay})
                    time.sleep(INGEST_POLL_MIN_SECONDS)
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
