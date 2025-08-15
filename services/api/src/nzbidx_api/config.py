"""Environment configuration helpers."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import List, Set

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%r, using default %s", name, value, default)
        return default


def api_keys() -> Set[str]:
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
def cors_origins() -> List[str]:
    value = os.getenv("CORS_ORIGINS", "")
    return [v.strip() for v in value.split(",") if v.strip()]


@lru_cache()
def nzb_timeout_seconds() -> int:
    """Maximum seconds to wait for NZB generation."""
    return _int_env("NZB_TIMEOUT_SECONDS", 30)


@lru_cache()
def search_timeout_ms() -> int:
    """Request timeout in milliseconds for OpenSearch calls."""
    return _int_env("SEARCH_TIMEOUT_MS", 2500)


@lru_cache()
def ilm_delete_days() -> int:
    """Retention period for OpenSearch indices."""
    return _int_env("ILM_DELETE_DAYS", 180)


@lru_cache()
def ilm_warm_days() -> int:
    """Age in days before indices move to the warm phase."""
    return _int_env("ILM_WARM_DAYS", 14)


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


@lru_cache()
def os_primary_shards() -> int:
    return _int_env("OS_PRIMARY_SHARDS", 1)


@lru_cache()
def os_replicas() -> int:
    return _int_env("OS_REPLICAS", 0)
