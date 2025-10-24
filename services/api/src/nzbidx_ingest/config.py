"""Configuration helpers for the ingest worker."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from importlib import resources
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
    base: float = 1.0
    max_delay: float = 60.0


def nntp_settings() -> NNTPSettings:
    """Return NNTP connection settings loaded from the environment."""

    host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
    port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
    ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
    use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563
    base = float(os.getenv("NNTP_CONNECT_BASE", "1"))
    max_delay = float(os.getenv("NNTP_CONNECT_MAX_DELAY", "60"))
    return NNTPSettings(
        host=host,
        port=port,
        use_ssl=use_ssl,
        user=os.getenv("NNTP_USER"),
        password=os.getenv("NNTP_PASS"),
        base=base,
        max_delay=max_delay,
    )


NNTP_SETTINGS: NNTPSettings = nntp_settings()


from .nntp_client import NNTPClient  # noqa: E402


NNTP_GROUP_WILDCARD: str = os.getenv("NNTP_GROUP_WILDCARD", "alt.binaries.*")

# Curated list of Binsearch-compatible alt.binaries groups.
BINSEARCH_GROUPS: tuple[str, ...] = (
    "alt.binaries.a51",
    "alt.binaries.alt",
    "alt.binaries.amazing",
    "alt.binaries.anime",
    "alt.binaries.ath",
    "alt.binaries.bloaf",
    "alt.binaries.blu-ray",
    "alt.binaries.boneless",
    "alt.binaries.brg",
    "alt.binaries.cd.image",
    "alt.binaries.cd.image.ps2.dvdiso",
    "alt.binaries.chello",
    "alt.binaries.comics.dcp",
    "alt.binaries.comp",
    "alt.binaries.coolkidweb",
    "alt.binaries.cores",
    "alt.binaries.department.pron",
    "alt.binaries.documentaries.french",
    "alt.binaries.drwho",
    "alt.binaries.dvd",
    "alt.binaries.e-book",
    "alt.binaries.e-book.flood",
    "alt.binaries.e-book.german",
    "alt.binaries.e-book.magazines",
    "alt.binaries.e-book.rpg",
    "alt.binaries.e-books",
    "alt.binaries.ebook",
    "alt.binaries.ebook.french",
    "alt.binaries.ebook.german",
    "alt.binaries.ebooks.german",
    "alt.binaries.encrypted",
    "alt.binaries.erotica",
    "alt.binaries.erotica.pornstars.80s",
    "alt.binaries.etc",
    "alt.binaries.faded-glory",
    "alt.binaries.flowed",
    "alt.binaries.font",
    "alt.binaries.frogs",
    "alt.binaries.ftn",
    "alt.binaries.games",
    "alt.binaries.ghosts",
    "alt.binaries.hdtv.x264",
    "alt.binaries.holiday",
    "alt.binaries.ijsklontje",
    "alt.binaries.inner-sanctum",
    "alt.binaries.kenpsx",
    "alt.binaries.misc",
    "alt.binaries.mom",
    "alt.binaries.movies",
    "alt.binaries.movies.divx",
    "alt.binaries.mp3",
    "alt.binaries.mp3.audiobooks.repost",
    "alt.binaries.mpeg.video.music",
    "alt.binaries.multimedia",
    "alt.binaries.multimedia.alias",
    "alt.binaries.multimedia.anime.highspeed",
    "alt.binaries.multimedia.erotica.amateur",
    "alt.binaries.music.classical",
    "alt.binaries.newznzb.alpha",
    "alt.binaries.newznzb.charlie",
    "alt.binaries.newznzb.delta",
    "alt.binaries.newznzb.oscar",
    "alt.binaries.newznzb.romeo",
    "alt.binaries.newznzb.sierra",
    "alt.binaries.nl",
    "alt.binaries.nordic.password.protected",
    "alt.binaries.nospam.female.bodyhair.pubes",
    "alt.binaries.pictures.earlmiller",
    "alt.binaries.pwp",
    "alt.binaries.rar.pw-required",
    "alt.binaries.scary.exe.files",
    "alt.binaries.sounds.flac",
    "alt.binaries.sounds.lossless.24bit",
    "alt.binaries.sounds.lossless.jazz",
    "alt.binaries.sounds.mp3",
    "alt.binaries.sounds.mp3.1950s",
    "alt.binaries.sounds.mp3.1970s",
    "alt.binaries.sounds.mp3.ambient",
    "alt.binaries.sounds.mp3.classical",
    "alt.binaries.sounds.mp3.complete_cd",
    "alt.binaries.sounds.mp3.dance",
    "alt.binaries.sounds.mp3.holland",
    "alt.binaries.superman",
    "alt.binaries.swedish",
    "alt.binaries.teevee",
    "alt.binaries.test",
    "alt.binaries.town",
    "alt.binaries.tv",
    "alt.binaries.tv.deutsch",
    "alt.binaries.tv.swedish",
    "alt.binaries.u-4all",
    "alt.binaries.usenet2day",
    "alt.binaries.warez",
    "alt.binaries.warez.quebec-hackers",
    "alt.binaries.warez.uk",
    "alt.binaries.welovelori",
    "alt.binaries.wood",
    "alt.binaries.x",
)


def _parse_group_list(raw: str) -> List[str]:
    parts = re.split(r"[\n,]", raw)
    return [g.strip() for g in parts if g.strip()]


def _resolve_group_mode() -> str:
    """Return the active group selection mode."""

    mode_env = (os.getenv("NNTP_GROUP_MODE") or "").strip().lower()
    valid_modes = {"curated", "configured", "auto"}
    if mode_env:
        if mode_env not in valid_modes:
            logger.warning(
                "unknown_group_mode",
                extra={"event": "unknown_group_mode", "mode": mode_env},
            )
        else:
            return mode_env

    if os.getenv("NNTP_GROUPS") or os.getenv("NNTP_GROUP_FILE"):
        return "configured"
    if mode_env:
        # Unknown mode falls back to curated by default.
        return "curated"
    return "curated"


def is_curated_mode() -> bool:
    """Return ``True`` when the curated group list is active."""

    return _resolve_group_mode() == "curated"


def _load_curated_groups() -> List[str]:
    env = os.getenv("NNTP_CURATED_GROUPS", "").strip()
    if env:
        groups = _parse_group_list(env)
        logger.info(
            "Using curated NNTP groups from environment",  # pragma: no cover - logging
            extra={"event": "ingest_groups_curated_env", "groups": groups},
        )
        return groups

    cfg = os.getenv("NNTP_CURATED_GROUP_FILE")
    if cfg:
        try:
            groups = _parse_group_list(Path(cfg).read_text(encoding="utf-8"))
        except OSError:
            logger.warning(
                "curated_group_file_unavailable",
                extra={"event": "curated_group_file_unavailable", "path": cfg},
            )
        else:
            if groups:
                logger.info(
                    "Using curated NNTP groups from file",  # pragma: no cover - logging
                    extra={
                        "event": "ingest_groups_curated_file",
                        "groups": groups,
                        "path": cfg,
                    },
                )
            return groups

    try:
        data = (
            resources.files(__package__)
            .joinpath("curated_groups.txt")
            .read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError):
        logger.warning(
            "curated_group_file_missing", extra={"event": "curated_group_file_missing"}
        )
        return []

    groups = _parse_group_list(data)
    if groups:
        logger.info(
            "Using packaged curated NNTP groups",  # pragma: no cover - logging
            extra={"event": "ingest_groups_curated_packaged", "groups": groups},
        )
    return groups


def _configured_groups() -> List[str]:
    env = os.getenv("NNTP_GROUPS", "")
    if not env:
        cfg = os.getenv("NNTP_GROUP_FILE")
        if cfg:
            try:
                env = Path(cfg).read_text(encoding="utf-8")
            except OSError:
                env = ""
    if env:
        groups = _parse_group_list(env)
        logger.info(
            "Using configured NNTP groups: %s",
            groups,
            extra={"event": "ingest_groups_config", "groups": groups},
        )
        return groups
    return []


def _load_groups() -> List[str]:
    mode = _resolve_group_mode()
    if mode == "curated":
        curated = _load_curated_groups()
        if curated:
            return curated
        logger.warning("curated_groups_empty", extra={"event": "curated_groups_empty"})

    groups = _configured_groups()
    if groups:
        return groups

    if mode == "configured":
        return []

    curated = list(BINSEARCH_GROUPS)
    client = NNTPClient(NNTP_SETTINGS)
    discovered = client.list_groups(NNTP_GROUP_WILDCARD)
    if discovered:
        available = {name for name in discovered}
        groups = [group for group in curated if group in available]
        missing = sorted(set(curated) - available)
        if groups:
            logger.info(
                "Using %d curated NNTP groups discovered on server",
                len(groups),
                extra={
                    "event": "ingest_groups_curated",
                    "groups": groups,
                    "missing_groups": missing,
                },
            )
            return groups
        logger.warning(
            "No curated NNTP groups available on server",
            extra={"event": "ingest_groups_curated_empty", "missing_groups": missing},
        )
    else:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        logger.warning("ingest_groups_missing", extra={"host": host})

    logger.info(
        "Falling back to curated Binsearch group list",
        extra={"event": "ingest_groups_curated_fallback", "groups": curated},
    )
    return curated


NNTP_GROUPS: List[str] | None = None


def get_nntp_groups() -> List[str]:
    """Return configured NNTP groups loading them on first use."""

    global NNTP_GROUPS
    if NNTP_GROUPS is None:
        NNTP_GROUPS = _load_groups()
    return NNTP_GROUPS


def set_nntp_groups(groups: List[str] | None) -> None:
    """Set cached NNTP groups."""

    global NNTP_GROUPS
    NNTP_GROUPS = groups


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
    Only a handful of top level categories are supported. Movies, TV, and
    adult releases default to ``104857600`` bytes (``100`` MB). Unspecified
    categories default to ``0`` which disables filtering for that category.
    """

    env_map = {
        "1000": int(os.getenv("CONSOLE_MIN_SIZE", "0")),
        "2000": int(os.getenv("MOVIES_MIN_SIZE", "104857600")),
        "3000": int(os.getenv("AUDIO_MIN_SIZE", "0")),
        "4000": int(os.getenv("PC_MIN_SIZE", "0")),
        "5000": int(os.getenv("TV_MIN_SIZE", "104857600")),
        "6000": int(os.getenv("XXX_MIN_SIZE", "104857600")),
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
