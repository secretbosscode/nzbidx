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
)
from . import config, cursors
from .nntp_client import NNTPClient
from .parsers import normalize_subject, detect_language
from .main import (
    insert_release,
    index_release,
    _infer_category,
    connect_db,
    connect_opensearch,
    CATEGORY_MAP,
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


def run_once() -> None:
    """Process a single batch for each configured NNTP group."""
    groups = config.NNTP_GROUPS or config._load_groups()
    if not groups:
        logger.info("ingest_no_groups")
        return
    config.NNTP_GROUPS = groups
    logger.info("ingest_groups", extra={"count": len(groups), "groups": groups})

    client = NNTPClient()
    client.connect()
    db = connect_db()
    os_client = connect_opensearch()

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
            inserted = insert_release(db, dedupe_key, category, language, tags)
            os_latency = 0.0
            if inserted:
                metrics["inserted"] += 1
                os_start = time.monotonic()
                index_release(
                    os_client,
                    dedupe_key,
                    category=category,
                    language=language,
                    tags=tags,
                )
                os_latency = time.monotonic() - os_start
                last_os_latency = os_latency
                metrics["indexed"] += 1
            latency = time.monotonic() - start_idx
            if os_breaker.is_open():
                time.sleep(CB_RESET_SECONDS / 2)
            elif os_latency * 1000 > INGEST_OS_LATENCY_MS:
                time.sleep(min(os_latency / 2, 2))
            elif latency > 0.5:
                time.sleep(min(latency, 5))
            current = idx
        cursors.set_cursor(group, current)
        metrics["deduped"] = metrics["processed"] - metrics["inserted"]
        duration_s = time.monotonic() - batch_start
        metrics["duration_ms"] = int(duration_s * 1000)
        metrics["os_latency_ms"] = int(last_os_latency * 1000)
        metrics["cursor"] = current
        metrics["high_water"] = high
        remaining = max(high - current, 0)
        metrics["remaining"] = remaining
        if duration_s > 0 and metrics["processed"] > 0 and remaining > 0:
            rate = metrics["processed"] / duration_s
            metrics["eta_s"] = int(remaining / rate)
        logger.info("ingest_batch", extra=metrics)


def run_forever(stop_event: Event | None = None) -> None:
    """Continuously poll groups until ``stop_event`` is set."""
    while not (stop_event and stop_event.is_set()):
        run_once()
        if stop_event:
            if stop_event.wait(INGEST_POLL_SECONDS):
                break
        else:
            time.sleep(INGEST_POLL_SECONDS)
