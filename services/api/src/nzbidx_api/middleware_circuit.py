"""Circuit breaker and retry helpers for external dependencies."""

from __future__ import annotations

import logging
import random
import time
import asyncio
import inspect
from typing import Callable, Generic, TypeVar, Awaitable

from .config import settings
from .metrics_log import inc_breaker_open
from .otel import set_span_attr, start_span

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
        self._lock = asyncio.Lock()

    def _state_unlocked(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.monotonic() - self._opened_at > self.reset_seconds:
            return "half-open"
        return "open"

    async def _state(self) -> str:
        async with self._lock:
            return self._state_unlocked()

    async def state(self) -> str:
        """Public accessor for the current breaker state."""
        return await self._state()

    async def is_open(self) -> bool:
        return (await self._state()) == "open"

    def _record_failure_unlocked(self) -> None:
        self._failures += 1
        if self._failures >= self.max_failures:
            self._opened_at = time.monotonic()
            logger.warning(
                "circuit_open",
                extra={"breaker_state": "open"},
            )

    async def _record_failure(self) -> None:
        async with self._lock:
            self._record_failure_unlocked()

    def _record_success_unlocked(self) -> None:
        self._failures = 0
        self._opened_at = None

    async def _record_success(self) -> None:
        async with self._lock:
            self._record_success_unlocked()

    async def record_success(self) -> None:
        """Public helper to record a successful call."""
        await self._record_success()

    async def record_failure(self) -> None:
        """Public helper to record a failed call."""
        await self._record_failure()

    async def call(self, func: Callable[..., Awaitable[T] | T], *args, **kwargs) -> T:
        async with self._lock:
            if self._state_unlocked() == "open":
                raise CircuitOpenError("circuit open")
            try:
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            except Exception:
                self._record_failure_unlocked()
                raise
            self._record_success_unlocked()
            return result


def call_with_retry(
    breaker: CircuitBreaker[T],
    dep: str,
    func: Callable[..., Awaitable[T] | T],
    *args,
    **kwargs,
) -> T:
    """Synchronous helper wrapping ``call_with_retry_async``."""

    return asyncio.run(
        call_with_retry_async(breaker, dep, func, *args, **kwargs)
    )


async def call_with_retry_async(
    breaker: CircuitBreaker[T],
    dep: str,
    func: Callable[..., Awaitable[T] | T],
    *args,
    **kwargs,
) -> T:
    """Async wrapper around ``func`` with retries and circuit breaker."""

    retries = settings.retry_max
    delay = settings.retry_base_ms / 1000
    attempt = 0
    while True:
        try:
            with start_span("dep_call"):
                set_span_attr("dep", dep)
                if await breaker.is_open():
                    raise CircuitOpenError("circuit open")
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
                await breaker.record_success()
                state = "open" if await breaker.is_open() else "closed"
                set_span_attr("breaker_state", state)
            logger.info(
                "dep_call",
                extra={
                    "dep": dep,
                    "retries": attempt,
                    "breaker_state": state,
                },
            )
            return result
        except CircuitOpenError:
            set_span_attr("breaker_state", "open")
            logger.warning(
                "dep_unavailable",
                extra={"dep": dep, "retries": attempt, "breaker_state": "open"},
            )
            inc_breaker_open(dep)
            raise
        except Exception:
            await breaker.record_failure()
            state = "half-open" if await breaker.is_open() else "closed"
            set_span_attr("breaker_state", state)
            if await breaker.is_open() or attempt >= retries:
                logger.warning(
                    "dep_fail",
                    extra={"dep": dep, "retries": attempt, "breaker_state": state},
                )
                raise
            jitter = random.uniform(0, settings.retry_jitter_ms / 1000)
            await asyncio.sleep(delay + jitter)
            delay *= 2
            attempt += 1


os_breaker: CircuitBreaker[object] = CircuitBreaker(
    max_failures=settings.cb_failure_threshold,
    reset_seconds=settings.cb_reset_seconds,
)
