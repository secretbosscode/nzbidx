"""Configuration helpers for the ingest worker."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class NNTPSettings:
    """Connection parameters for an NNTP server."""

    host: str | None
    port: int
    use_ssl: bool
    user: str | None
    password: str | None


def nntp_settings() -> NNTPSettings:
    """Return NNTP connection settings loaded from the environment."""

    host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
    port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
    ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
    use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563
    return NNTPSettings(
        host=host,
        port=port,
        use_ssl=use_ssl,
        user=os.getenv("NNTP_USER"),
        password=os.getenv("NNTP_PASS"),
    )


NNTP_SETTINGS: NNTPSettings = nntp_settings()


from .nntp_client import NNTPClient  # noqa: E402


NNTP_GROUP_WILDCARD: str = os.getenv("NNTP_GROUP_WILDCARD", "alt.binaries.*")


def _load_groups() -> List[str]:
    env = os.getenv("NNTP_GROUPS", "")
    if not env:
        cfg = os.getenv("NNTP_GROUP_FILE")
        if cfg:
            try:
                env = Path(cfg).read_text(encoding="utf-8")
            except OSError:
                env = ""
    if env:
        parts = re.split(r"[\n,]", env)
        groups = [g.strip() for g in parts if g.strip()]
        logger.info(
            "Using configured NNTP groups: %s",
            groups,
            extra={"event": "ingest_groups_config", "groups": groups},
        )
        return groups

    client = NNTPClient(NNTP_SETTINGS)
    groups = client.list_groups(NNTP_GROUP_WILDCARD)
    if groups:
        logger.info(
            "Discovered %d NNTP groups from server",
            len(groups),
            extra={"event": "ingest_groups_discovered", "groups": groups},
        )
    else:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        logger.warning("ingest_groups_missing", extra={"host": host})
    return groups


NNTP_GROUPS: List[str] = _load_groups()


def _load_ignore_groups() -> List[str]:
    env = os.getenv("NNTP_IGNORE_GROUPS", "")
    if env:
        groups = [g.strip() for g in env.split(",") if g.strip()]
        logger.info(
            "Ignoring NNTP groups per configuration: %s",
            groups,
            extra={"event": "ingest_ignore_groups_config", "groups": groups},
        )
        return groups
    return []


IGNORE_GROUPS: List[str] = _load_ignore_groups()
# Benchmarks showed a batch size of 1000 with a polling interval between
# 5s and 60s provided the best ingest throughput without increasing load.
INGEST_BATCH: int = int(os.getenv("INGEST_BATCH", "1000"))
# Dynamic batch sizing is bounded by configurable minimum/maximum limits.
INGEST_BATCH_MIN: int = int(os.getenv("INGEST_BATCH_MIN", "100"))
INGEST_BATCH_MAX: int = int(os.getenv("INGEST_BATCH_MAX", str(INGEST_BATCH)))
INGEST_POLL_MIN_SECONDS: int = int(os.getenv("INGEST_POLL_MIN_SECONDS", "5"))
INGEST_POLL_MAX_SECONDS: int = int(os.getenv("INGEST_POLL_MAX_SECONDS", "60"))
DETECT_LANGUAGE: bool = os.getenv("DETECT_LANGUAGE", "1").lower() in {
    "1",
    "true",
    "yes",
}
AUDIO_EXTENSIONS: list[str] = [
    ext.strip().upper()
    for ext in os.getenv(
        "AUDIO_EXTENSIONS",
        "FLAC,MP3,AAC,M4A,WAV,OGG,WMA",
    ).split(",")
    if ext.strip()
]
BOOK_EXTENSIONS: list[str] = [
    ext.strip().upper()
    for ext in os.getenv(
        "BOOK_EXTENSIONS",
        "EPUB,MOBI,PDF,AZW3,CBZ,CBR",
    ).split(",")
    if ext.strip()
]
ALLOWED_MOVIE_EXTENSIONS: list[str] = [
    ext.strip().lower()
    for ext in os.getenv(
        "ALLOWED_MOVIE_EXTENSIONS",
        "mkv,mp4,mov,m4v,mpg,mpeg,avi,flv,webm,wmv,vob,evo,iso,m2ts,ts",
    ).split(",")
    if ext.strip()
]
ALLOWED_TV_EXTENSIONS: list[str] = [
    ext.strip().lower()
    for ext in os.getenv(
        "ALLOWED_TV_EXTENSIONS",
        "mkv,mp4,mov,m4v,mpg,mpeg,avi,flv,webm,wmv,vob,evo,iso,m2ts,ts",
    ).split(",")
    if ext.strip()
]
ALLOWED_ADULT_EXTENSIONS: list[str] = [
    ext.strip().lower()
    for ext in os.getenv(
        "ALLOWED_ADULT_EXTENSIONS",
        "mkv,mp4,mov,m4v,mpg,mpeg,avi,flv,webm,wmv,vob,evo,iso,m2ts,ts",
    ).split(",")
    if ext.strip()
]
CURSOR_DB: str = os.getenv("CURSOR_DB") or os.getenv("DATABASE_URL", "./cursors.sqlite")
CB_RESET_SECONDS: int = int(os.getenv("CB_RESET_SECONDS", "30"))
# Base delay applied when database latency exceeds thresholds. Set to ``0`` to
# disable adaptive backoff.
INGEST_SLEEP_MS: int = int(os.getenv("INGEST_SLEEP_MS", "1000"))
INGEST_DB_LATENCY_MS: int = int(os.getenv("INGEST_DB_LATENCY_MS", "1200"))
# Emit ingest batch metrics at INFO level every N batches. Set to 0 to disable.
INGEST_LOG_EVERY: int = int(os.getenv("INGEST_LOG_EVERY", "100"))
RELEASE_PART_MAX_RELEASES: int = int(os.getenv("RELEASE_PART_MAX_RELEASES", "100000"))
# Enable strict segment schema validation when set to a truthy value.
# Disabled by default to reduce ingest overhead in production.
VALIDATE_SEGMENTS: bool = os.getenv("VALIDATE_SEGMENTS", "").lower() in {
    "1",
    "true",
    "yes",
}


def _load_category_min_sizes() -> dict[str, int]:
    """Return per-category minimum size thresholds from the environment.

    Values are configured via ``<CATEGORY>_MIN_SIZE`` variables where
    ``CATEGORY`` is the upper-case Newznab style label (e.g. ``MOVIES``).
    Only a handful of top level categories are supported; unspecified values
    default to ``0`` which disables filtering for that category.
    """

    env_map = {
        "1000": int(os.getenv("CONSOLE_MIN_SIZE", "0")),
        "2000": int(os.getenv("MOVIES_MIN_SIZE", "0")),
        "3000": int(os.getenv("AUDIO_MIN_SIZE", "0")),
        "4000": int(os.getenv("PC_MIN_SIZE", "0")),
        "5000": int(os.getenv("TV_MIN_SIZE", "0")),
        "6000": int(os.getenv("XXX_MIN_SIZE", "0")),
        "7000": int(os.getenv("BOOKS_MIN_SIZE", os.getenv("OTHER_MIN_SIZE", "0"))),
    }
    return env_map


CATEGORY_MIN_SIZES: dict[str, int] = _load_category_min_sizes()


def _parse_release_min_sizes() -> tuple[
    dict[str, int], list[tuple[re.Pattern[str], int]]
]:
    """Parse ``RELEASE_MIN_SIZES`` into exact and regex mappings.

    The environment variable accepts comma separated ``pattern=size`` pairs. A
    pattern wrapped in forward slashes is treated as a regular expression;
    otherwise an exact, normalized title match is used.
    """

    raw = os.getenv("RELEASE_MIN_SIZES", "").strip()
    exact: dict[str, int] = {}
    regex: list[tuple[re.Pattern[str], int]] = []
    if not raw:
        return exact, regex
    for item in raw.split(","):
        if not item.strip():
            continue
        try:
            pattern, size_str = item.split("=", 1)
            size = int(size_str.strip())
        except ValueError:
            logger.warning("invalid RELEASE_MIN_SIZES entry: %r", item)
            continue
        pattern = pattern.strip()
        if pattern.startswith("/") and pattern.endswith("/"):
            try:
                regex.append((re.compile(pattern[1:-1]), size))
            except re.error:
                logger.warning("invalid RELEASE_MIN_SIZES regex: %s", pattern)
        else:
            exact[pattern] = size
    return exact, regex


RELEASE_MIN_EXACT, RELEASE_MIN_REGEX = _parse_release_min_sizes()


def min_size_for_release(norm_title: str, category: str) -> int:
    """Return the minimum size threshold for ``norm_title`` and ``category``.

    ``category`` should be a numeric category ID string. Overrides defined via
    ``RELEASE_MIN_SIZES`` take precedence; otherwise the base category's
    configured default is used.
    """

    override = RELEASE_MIN_EXACT.get(norm_title)
    if override is None:
        for pattern, size in RELEASE_MIN_REGEX:
            if pattern.search(norm_title):
                override = size
                break
    if override is not None:
        return override

    try:
        base_cat = str(int(category) // 1000 * 1000)
    except Exception:
        base_cat = "7000"  # fall back to "other"
    return CATEGORY_MIN_SIZES.get(base_cat, 0)
