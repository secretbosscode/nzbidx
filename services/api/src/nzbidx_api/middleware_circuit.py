from __future__ import annotations

import random
import time
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    """Raised when the circuit breaker is open."""


class CircuitBreaker(Generic[T]):
    """Very small circuit breaker with retry and jitter."""

    def __init__(
        self, *, max_failures: int = 3, reset_seconds: float = 30.0, retries: int = 2
    ) -> None:
        self.max_failures = max_failures
        self.reset_seconds = reset_seconds
        self.retries = retries
        self._failures = 0
        self._opened_at: float | None = None

    def _opened(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at > self.reset_seconds:
            self._opened_at = None
            self._failures = 0
            return False
        return True

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        if self._opened():
            raise CircuitOpenError("circuit open")
        last_exc: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                result = func(*args, **kwargs)
                self._failures = 0
                return result
            except Exception as exc:  # pragma: no cover - network errors
                last_exc = exc
                time.sleep(random.uniform(0.05, 0.1))
        self._failures += 1
        if self._failures >= self.max_failures:
            self._opened_at = time.monotonic()
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("unknown failure")

    def is_open(self) -> bool:
        return self._opened()


os_breaker: CircuitBreaker[object] = CircuitBreaker()
redis_breaker: CircuitBreaker[object] = CircuitBreaker()
