"""Circuit breaker and retry helpers for external dependencies."""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Generic, TypeVar

from .config import (
    cb_failure_threshold,
    cb_reset_seconds,
    retry_base_ms,
    retry_jitter_ms,
    retry_max,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


class CircuitOpenError(RuntimeError):
    """Raised when the circuit breaker is open."""


class CircuitBreaker(Generic[T]):
    """Simple circuit breaker with half-open probing."""

    def __init__(self, *, max_failures: int, reset_seconds: float) -> None:
        self.max_failures = max_failures
        self.reset_seconds = reset_seconds
        self._failures = 0
        self._opened_at: float | None = None

    def _state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.monotonic() - self._opened_at > self.reset_seconds:
            return "half-open"
        return "open"

    def is_open(self) -> bool:
        return self._state() == "open"

    def _record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.max_failures:
            self._opened_at = time.monotonic()
            logger.warning(
                "circuit_open",
                extra={"breaker_state": "open"},
            )

    def _record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        state = self._state()
        if state == "open":
            raise CircuitOpenError("circuit open")
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result


def call_with_retry(
    breaker: CircuitBreaker[T],
    dep: str,
    func: Callable[..., T],
    *args,
    **kwargs,
) -> T:
    """Call ``func`` with jittered retries and circuit breaker tracking."""

    retries = retry_max()
    delay = retry_base_ms() / 1000
    attempt = 0
    while True:
        try:
            result = breaker.call(func, *args, **kwargs)
            logger.info(
                "dep_call",
                extra={
                    "dep": dep,
                    "retries": attempt,
                    "breaker_state": "closed" if not breaker.is_open() else "open",
                },
            )
            return result
        except CircuitOpenError:
            logger.warning(
                "dep_unavailable",
                extra={"dep": dep, "retries": attempt, "breaker_state": "open"},
            )
            raise
        except Exception:
            if attempt >= retries:
                logger.warning(
                    "dep_fail",
                    extra={
                        "dep": dep,
                        "retries": attempt,
                        "breaker_state": "half-open" if breaker.is_open() else "closed",
                    },
                )
                raise
            jitter = random.uniform(0, retry_jitter_ms() / 1000)
            time.sleep(delay + jitter)
            delay *= 2
            attempt += 1


os_breaker: CircuitBreaker[object] = CircuitBreaker(
    max_failures=cb_failure_threshold(),
    reset_seconds=cb_reset_seconds(),
)

redis_breaker: CircuitBreaker[object] = CircuitBreaker(
    max_failures=cb_failure_threshold(),
    reset_seconds=cb_reset_seconds(),
)
