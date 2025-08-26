"""Environment configuration helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache

logger = logging.getLogger(__name__)


NNTP_GROUPS: list[str] = []


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%r, using default %s", name, value, default)
        return default


def api_keys() -> set[str]:
    keys = os.getenv("API_KEYS", "")
    return {k.strip() for k in keys.split(",") if k.strip()}


@dataclass
class Settings:
    """Integer-based configuration settings loaded from the environment."""

    search_ttl_seconds: int = field(
        default_factory=lambda: _int_env("SEARCH_TTL_SECONDS", 60)
    )
    search_cache_max_entries: int = field(
        default_factory=lambda: _int_env("SEARCH_CACHE_MAX_ENTRIES", 1024)
    )
    rate_limit: int = field(default_factory=lambda: _int_env("RATE_LIMIT", 60))
    rate_window: int = field(default_factory=lambda: _int_env("RATE_WINDOW", 60))
    key_rate_limit: int = field(default_factory=lambda: _int_env("KEY_RATE_LIMIT", 100))
    key_rate_window: int = field(
        default_factory=lambda: _int_env("KEY_RATE_WINDOW", 60)
    )
    max_request_bytes: int = field(
        default_factory=lambda: _int_env("MAX_REQUEST_BYTES", 1_048_576)
    )
    max_query_bytes: int = field(
        default_factory=lambda: _int_env("MAX_QUERY_BYTES", 2048)
    )
    max_param_bytes: int = field(
        default_factory=lambda: _int_env("MAX_PARAM_BYTES", 256)
    )
    nntp_timeout_seconds: int = field(
        default_factory=lambda: _int_env("NNTP_TIMEOUT", 30)
    )
    nntp_total_timeout_seconds: int = field(
        default_factory=lambda: _int_env("NNTP_TOTAL_TIMEOUT", 600)
    )
    nzb_timeout_seconds: int = 0
    nzb_max_segments: int = field(
        default_factory=lambda: _int_env("NZB_MAX_SEGMENTS", 1000)
    )
    cb_failure_threshold: int = field(
        default_factory=lambda: _int_env("CB_FAILURE_THRESHOLD", 5)
    )
    cb_reset_seconds: int = field(
        default_factory=lambda: _int_env("CB_RESET_SECONDS", 30)
    )
    retry_max: int = field(default_factory=lambda: _int_env("RETRY_MAX", 2))
    retry_base_ms: int = field(default_factory=lambda: _int_env("RETRY_BASE_MS", 50))
    retry_jitter_ms: int = field(
        default_factory=lambda: _int_env("RETRY_JITTER_MS", 200)
    )
    max_limit: int = field(default_factory=lambda: _int_env("MAX_LIMIT", 100))
    max_offset: int = field(default_factory=lambda: _int_env("MAX_OFFSET", 10_000))

    def __post_init__(self) -> None:
        timeout = _int_env("NZB_TIMEOUT_SECONDS", self.nntp_total_timeout_seconds)
        if timeout < self.nntp_total_timeout_seconds:
            logger.warning(
                "NZB_TIMEOUT_SECONDS (%s) < NNTP_TOTAL_TIMEOUT (%s); using %s",
                timeout,
                self.nntp_total_timeout_seconds,
                self.nntp_total_timeout_seconds,
            )
        self.nzb_timeout_seconds = max(timeout, self.nntp_total_timeout_seconds)

    def reload(self) -> None:
        """Reload settings from the current environment."""
        new = type(self)()
        self.__dict__.update(vars(new))


settings = Settings()


def nntp_timeout_seconds() -> int:
    """Backwards-compatible accessor for the NNTP timeout."""
    return settings.nntp_timeout_seconds


@lru_cache()
def cors_origins() -> list[str]:
    value = os.getenv("CORS_ORIGINS", "")
    return [v.strip() for v in value.split(",") if v.strip()]


@lru_cache()
def strict_transport_security() -> str | None:
    """Strict-Transport-Security header value or ``None`` to disable."""

    value = os.getenv(
        "STRICT_TRANSPORT_SECURITY", "max-age=63072000; includeSubDomains"
    ).strip()
    return value or None


@lru_cache()
def request_id_header() -> str:
    """Header name used for request correlation."""
    return os.getenv("REQUEST_ID_HEADER", "X-Request-ID")


def validate_nntp_config() -> list[str]:
    """Check required NNTP configuration variables.

    Returns a list of any missing variables after logging an error. This
    allows callers to gracefully handle misconfiguration before attempting to
    perform NNTP operations.
    """

    required = ["NNTP_HOST", "NNTP_PORT", "NNTP_USER", "NNTP_PASS"]
    missing = [name for name in required if not os.getenv(name)]

    env_groups = os.getenv("NNTP_GROUPS")
    global NNTP_GROUPS
    if env_groups:
        NNTP_GROUPS = [g.strip() for g in env_groups.split(",") if g.strip()]
    else:
        from nzbidx_ingest.config import _load_groups

        NNTP_GROUPS = _load_groups()

    if missing:
        logger.error("missing NNTP configuration: %s", ", ".join(missing))
    return missing
