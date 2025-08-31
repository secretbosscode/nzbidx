"""Header-only ingest loop."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from threading import Event

from nzbidx_api.json_utils import orjson as json

from .config import (
    INGEST_BATCH_MIN,
    INGEST_BATCH_MAX,
    INGEST_POLL_MIN_SECONDS,
    INGEST_POLL_MAX_SECONDS,
    INGEST_SLEEP_MS,
    INGEST_DB_LATENCY_MS,
    min_size_for_release,
)
from . import config, cursors
from .nntp_client import NNTPClient
from .parsers import (
    normalize_subject,
    detect_language,
    extract_segment_number,
    extract_file_extension,
)
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

try:  # pragma: no cover - optional dependency
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

logger = logging.getLogger(__name__)

# Track consecutive failures per group to allow backoff or alerting.
# This is reset on successful xover calls.
_group_failures: dict[str, int] = {}

# Track groups that failed to reconnect and when they should be probed next.
# The value is a monotonic timestamp after which the group should be retried.
_group_probes: dict[str, float] = {}

# Counter used to throttle how often batch metrics are logged at INFO level.
_log_counter = 0

# Monotonic timestamp of the last successful ingest iteration.
last_run: float = 0.0
# Wall-clock timestamp of the last successful ingest iteration.
last_run_wall: float = 0.0


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


def _clean_text(s: str) -> str:
    """Return ``s`` with surrogate code points and NUL bytes removed.

    Surrogate code points cannot be encoded in UTF-8 and will raise
    ``UnicodeEncodeError`` when encountered. Encoding with ``errors='ignore'``
    and decoding back to ``str`` drops any such characters.  NUL bytes are
    stripped separately since they can cause issues with some databases and
    tools.
    """

    return s.replace("\x00", "").encode("utf-8", errors="ignore").decode("utf-8")


def _process_groups(
    client: NNTPClient,
    db: object,
    groups: list[str],
    ignored: set[str],
) -> float:
    aggregate = _AggregateMetrics()
    db_errors: tuple[type[BaseException], ...] = ()
    if psycopg:
        db_errors = (psycopg.DataError,)

    for ig in ignored:
        prune_group(db, ig)

    for group in groups:
        for attempt in range(2):
            last = cursors.get_cursor(group) or 0
            start = last + 1
            high = 0
            if hasattr(client, "group"):
                try:
                    _resp, _count, _low, high_s, _name = client.group(group)
                    high = int(high_s)
                except Exception:
                    high = 0
            else:
                try:
                    high = int(client.high_water_mark(group))
                except Exception:
                    high = 0
            remaining = max(high - last, 0)
            headers: list[dict[str, object]] = []
            if remaining > 0:
                batch = min(remaining, INGEST_BATCH_MAX)
                batch = max(batch, min(remaining, INGEST_BATCH_MIN))
                end = start + batch - 1
                try:
                    headers = client.xover(group, start, end)
                    _group_failures[group] = 0
                    _group_probes.pop(group, None)
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
                    break
            if not headers:
                logger.info(
                    "ingest_idle",
                    extra={"group": group, "cursor": last, "high_water": high},
                )
                # ``high`` is ``0`` when the NNTP server is unreachable.
                # Attempt to reconnect once; if still unreachable schedule a
                # probe for a future run instead of blocking the loop.
                if high == 0:
                    if attempt == 0:
                        failures = _group_failures.get(group, 0) + 1
                        _group_failures[group] = failures
                        delay = min(INGEST_POLL_MAX_SECONDS, 2 ** (failures - 1))
                        logger.warning(
                            "ingest_reconnect",
                            extra={
                                "group": group,
                                "failures": failures,
                                "delay": delay,
                            },
                        )
                        try:
                            client.connect()
                        except Exception:
                            logger.exception(
                                "ingest_reconnect_failed",
                                extra={"group": group},
                            )
                        continue
                    probe_delay = min(
                        INGEST_POLL_MAX_SECONDS,
                        2 ** max(_group_failures.get(group, 1) - 1, 0),
                    )
                    _group_probes[group] = time.monotonic() + probe_delay
                    break
                if high > 0:
                    _group_failures[group] = 0
                    _group_probes.pop(group, None)
                    cursors.mark_irrelevant(group)
                break
        if not headers:
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
        parts: defaultdict[str, list[tuple[int, str, str, int]]] = defaultdict(list)
        for idx, header in enumerate(headers, start=start):
            metrics["processed"] += 1
            size = int(header.get("bytes") or header.get(":bytes") or 0)
            current = idx
            message_id = _clean_text(str(header.get("message-id") or "")).strip()
            if size <= 0 and message_id:
                size = client.body_size(message_id)
            if size <= 0:
                continue
            subject = _clean_text(str(header.get("subject", "")))
            norm_title, tags = normalize_subject(subject, with_tags=True)
            norm_title = _clean_text(norm_title)
            posted = header.get("date")
            day_bucket = ""
            posted_at = None
            if posted:
                try:
                    dt = parsedate_to_datetime(str(posted)).astimezone(timezone.utc)
                    posted_at = _clean_text(dt.isoformat())
                    day_bucket = dt.strftime("%Y-%m-%d")
                except Exception:
                    day_bucket = ""
            dedupe_key = _clean_text(
                f"{norm_title}:{day_bucket}" if day_bucket else norm_title
            )
            language = _clean_text(detect_language(subject) or "und")
            category = _clean_text(
                _infer_category(subject, str(group)) or CATEGORY_MAP["other"]
            )
            group_clean = _clean_text(str(group))
            ext = extract_file_extension(subject)
            allowed: set[str] | None = None
            try:
                cat_int = int(category)
                if 2000 <= cat_int < 3000:
                    allowed = config.ALLOWED_MOVIE_EXTENSIONS
                elif 5000 <= cat_int < 6000:
                    allowed = config.ALLOWED_TV_EXTENSIONS
                elif 6000 <= cat_int < 7000:
                    allowed = config.ALLOWED_ADULT_EXTENSIONS
            except Exception:
                allowed = None
            if allowed is not None and (not ext or ext not in allowed):
                continue
            tags = [_clean_text(tag) for tag in (tags or [])]
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
                    group_clean,
                    size,
                    posted_at,
                )
            if message_id:
                seg_num = extract_segment_number(subject)
                clean_id = _clean_text(message_id.strip("<>"))
                parts[dedupe_key].append((seg_num, clean_id, group_clean, size))
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
            placeholder = "?" if db.__class__.__module__.startswith("sqlite3") else "%s"

            def _update_segments(cur: object) -> None:
                for title, segs in parts.items():
                    if not segs:
                        continue
                    cat = releases.get(title)
                    group_name = cat[4] if cat else None
                    try:
                        cur.execute(
                            f"SELECT segments FROM release WHERE norm_title = {placeholder}",
                            (title,),
                        )
                        row = cur.fetchone()
                    except db_errors:  # type: ignore[misc]
                        logger.warning(
                            "segment_update_data_error",
                            extra={"norm_title": title, "group": group_name},
                        )
                        db.rollback()
                        continue
                    except Exception:
                        row = None
                    existing_segments = []
                    if row:
                        try:
                            existing_segments = json.loads(row[0] or "[]")
                        except Exception:
                            existing_segments = []
                    for seg in existing_segments:
                        seg["message_id"] = _clean_text(str(seg.get("message_id", "")))
                        seg["group"] = _clean_text(str(seg.get("group", "")))
                    validate_segment_schema(existing_segments)

                    # Deduplicate newly fetched segments by message-id before merging.
                    deduped: list[dict[str, int | str]] = []
                    seen_ids: set[str] = set()
                    for n, m, g, s in segs:
                        clean_m = _clean_text(m)
                        clean_g = _clean_text(g)
                        if clean_m in seen_ids:
                            continue
                        seen_ids.add(clean_m)
                        deduped.append(
                            {
                                "number": n,
                                "message_id": clean_m,
                                "group": clean_g,
                                "size": s,
                            },
                        )

                    existing_map = {seg["message_id"]: seg for seg in existing_segments}
                    for seg in deduped:
                        message_id = seg["message_id"]
                        existing_map.setdefault(message_id, seg)
                    combined_segments = list(existing_map.values())
                    validate_segment_schema(combined_segments)
                    total_size = sum(seg["size"] for seg in combined_segments)
                    part_counts[title] = len(combined_segments)
                    has_parts = bool(combined_segments)
                    cat = releases.get(title)
                    category_id = str(cat[1]) if cat else CATEGORY_MAP["other"]
                    min_bytes = min_size_for_release(title, category_id)
                    try:
                        if total_size < min_bytes:
                            cur.execute(
                                f"DELETE FROM release WHERE norm_title = {placeholder} AND category_id = {placeholder}",
                                (title, int(category_id)),
                            )
                            inserted.discard(title)
                            continue
                        cur.execute(
                            f"UPDATE release SET segments = {placeholder}, has_parts = {placeholder}, part_count = {placeholder}, size_bytes = {placeholder} WHERE norm_title = {placeholder}",
                            (
                                json.dumps(combined_segments).decode(),
                                has_parts,
                                part_counts[title],
                                total_size,
                                title,
                            ),
                        )
                    except db_errors:  # type: ignore[misc]
                        logger.warning(
                            "segment_update_data_error",
                            extra={"norm_title": title, "group": group_name},
                        )
                        db.rollback()
                        continue
                    has_parts_flags[title] = has_parts
                    changed.add(title)

            try:
                with db.cursor() as cur:
                    _update_segments(cur)
            except (AttributeError, TypeError):
                cur = db.cursor()
                try:
                    _update_segments(cur)
                finally:
                    cur.close()
            try:
                db.commit()
            except Exception:
                logger.exception("ingest_commit_error", extra={"group": group})
                raise

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


def run_once(client: NNTPClient | None = None) -> float:
    """Process a single batch for each configured NNTP group.

    ``client`` may be provided for tests; when ``None`` a new client is created
    using configured settings. Returns the suggested delay before the next
    poll.
    """
    global last_run, last_run_wall
    groups_all = config.get_nntp_groups()
    ignored = set(config.IGNORE_GROUPS or [])
    if ignored:
        logger.info("ingest_ignore_groups", extra={"groups": list(ignored)})
    groups_all = [g for g in groups_all if g not in ignored]
    if not groups_all:
        logger.info("ingest_no_groups")
        last_run = time.monotonic()
        last_run_wall = time.time()
        return INGEST_POLL_MAX_SECONDS
    skip = set(cursors.get_irrelevant_groups())
    if skip:
        groups_all = [g for g in groups_all if g not in skip]
    if not groups_all:
        logger.info("ingest_no_groups")
        last_run = time.monotonic()
        last_run_wall = time.time()
        return INGEST_POLL_MAX_SECONDS
    now = time.monotonic()
    groups: list[str] = []
    for g in groups_all:
        probe = _group_probes.get(g)
        if probe is not None and probe > now:
            logger.debug(
                "ingest_probe_pending", extra={"group": g, "next_probe": probe}
            )
            continue
        groups.append(g)
    config.set_nntp_groups(groups_all)
    if not groups:
        logger.info("ingest_no_groups")
        last_run = now
        last_run_wall = time.time()
        delay = INGEST_POLL_MAX_SECONDS
        if _group_probes:
            next_probe = min(_group_probes.values())
            delay = max(
                INGEST_POLL_MIN_SECONDS,
                min(INGEST_POLL_MAX_SECONDS, max(0.0, next_probe - now)),
            )
        return delay
    logger.info("ingest_groups", extra={"count": len(groups), "groups": groups})

    created_client = False
    if client is None:
        client = NNTPClient(config.NNTP_SETTINGS)
        created_client = True
    db = None
    try:
        if created_client:
            client.connect()
        db = connect_db()
        delay = _process_groups(client, db, groups, ignored)
        if _group_probes:
            now = time.monotonic()
            next_probe = min(_group_probes.values())
            probe_delay = max(0.0, next_probe - now)
            probe_delay = max(
                INGEST_POLL_MIN_SECONDS,
                min(INGEST_POLL_MAX_SECONDS, probe_delay),
            )
            delay = min(delay, probe_delay)
        last_run = time.monotonic()
        last_run_wall = time.time()
        return delay
    finally:
        if created_client:
            try:
                client.quit()
            except Exception:
                pass
        if db is not None:
            try:
                db.close()
            except Exception:
                pass


def run_forever(stop_event: Event | None = None) -> None:
    """Continuously poll groups until ``stop_event`` is set."""
    failure_delay = INGEST_POLL_MIN_SECONDS
    while not (stop_event and stop_event.is_set()):
        try:
            delay = run_once()
            failure_delay = INGEST_POLL_MIN_SECONDS
        except KeyboardInterrupt:  # pragma: no cover
            logger.info("ingest_loop_interrupted")
            raise
        except Exception:  # pragma: no cover
            logger.exception("ingest_loop_failure")
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
