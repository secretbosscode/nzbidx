"""Header-only ingest loop."""

from __future__ import annotations

import logging
import time

from .config import NNTP_GROUPS, INGEST_BATCH, INGEST_POLL_SECONDS
from . import cursors
from .nntp_client import NNTPClient
from .parsers import normalize_subject, detect_language
from .main import (
    insert_release,
    index_release,
    _infer_category,
    connect_db,
    connect_opensearch,
)
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


def run_once() -> None:
    """Process a single batch for each configured NNTP group."""
    client = NNTPClient()
    client.connect()
    db = connect_db()
    os_client = connect_opensearch()

    for group in NNTP_GROUPS:
        last = cursors.get_cursor(group) or 0
        start = last + 1
        end = start + INGEST_BATCH - 1
        headers = client.xover(group, start, end)
        if not headers:
            continue
        current = last
        for idx, header in enumerate(headers, start=start):
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
            language = detect_language(subject)
            category = _infer_category(subject)
            if insert_release(db, dedupe_key, category, language, tags):
                index_release(
                    os_client,
                    dedupe_key,
                    category=category,
                    language=language,
                    tags=tags,
                )
            current = idx
        cursors.set_cursor(group, current)


def run_forever() -> None:
    """Continuously poll groups according to ``INGEST_POLL_SECONDS``."""
    while True:
        run_once()
        time.sleep(INGEST_POLL_SECONDS)
