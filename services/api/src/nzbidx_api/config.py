"""Environment configuration helpers."""

from __future__ import annotations

import logging
import os
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


@lru_cache()
def search_ttl_seconds() -> int:
    return _int_env("SEARCH_TTL_SECONDS", 60)


def rate_limit() -> int:
    return _int_env("RATE_LIMIT", 60)


def rate_window() -> int:
    return _int_env("RATE_WINDOW", 60)


def key_rate_limit() -> int:
    return _int_env("KEY_RATE_LIMIT", 100)


def key_rate_window() -> int:
    return _int_env("KEY_RATE_WINDOW", 60)


@lru_cache()
def max_request_bytes() -> int:
    return _int_env("MAX_REQUEST_BYTES", 1_048_576)


def max_query_bytes() -> int:
    return _int_env("MAX_QUERY_BYTES", 2048)


def max_param_bytes() -> int:
    return _int_env("MAX_PARAM_BYTES", 256)


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
def nzb_timeout_seconds() -> int:
    """Maximum seconds to wait for NZB generation.

    Defaults to ``NNTP_TOTAL_TIMEOUT`` (``600`` seconds) and guarantees the
    returned timeout is at least as long as the NNTP total timeout. This
    avoids the API request timing out before the underlying NNTP operations
    complete. To override, set ``NZB_TIMEOUT_SECONDS`` to a value greater than
    or equal to ``NNTP_TOTAL_TIMEOUT``.
    """

    nntp_total = _int_env("NNTP_TOTAL_TIMEOUT", 600)
    timeout = _int_env("NZB_TIMEOUT_SECONDS", nntp_total)
    if timeout < nntp_total:
        logger.warning(
            "NZB_TIMEOUT_SECONDS (%s) < NNTP_TOTAL_TIMEOUT (%s); using %s",
            timeout,
            nntp_total,
            nntp_total,
        )
    return max(timeout, nntp_total)


@lru_cache()
def nzb_max_segments() -> int:
    """Maximum number of segments allowed in an NZB document."""

    return _int_env("NZB_MAX_SEGMENTS", 1000)


@lru_cache()
def nntp_timeout_seconds() -> int:
    """Connection timeout for NNTP operations."""
    return _int_env("NNTP_TIMEOUT", 30)


@lru_cache()
def nntp_total_timeout_seconds() -> int:
    """Total allowed time for NNTP operations across retries."""
    return _int_env("NNTP_TOTAL_TIMEOUT", 600)


@lru_cache()
def request_id_header() -> str:
    """Header name used for request correlation."""
    return os.getenv("REQUEST_ID_HEADER", "X-Request-ID")


@lru_cache()
def cb_failure_threshold() -> int:
    """Number of consecutive failures before the circuit trips."""
    return _int_env("CB_FAILURE_THRESHOLD", 5)


@lru_cache()
def cb_reset_seconds() -> int:
    """Seconds before a tripped circuit half-opens for probing."""
    return _int_env("CB_RESET_SECONDS", 30)


@lru_cache()
def retry_max() -> int:
    """Maximum number of retries for dependency calls."""
    return _int_env("RETRY_MAX", 2)


@lru_cache()
def retry_base_ms() -> int:
    """Base backoff in milliseconds for retries."""
    return _int_env("RETRY_BASE_MS", 50)


@lru_cache()
def retry_jitter_ms() -> int:
    """Additional random jitter applied to retries in milliseconds."""
    return _int_env("RETRY_JITTER_MS", 200)


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
