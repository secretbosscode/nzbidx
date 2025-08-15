from __future__ import annotations

import logging
import os
import threading
from collections import Counter
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_interval = int(os.getenv("METRICS_LOG_INTERVAL", "60"))
_counters: Counter[str] = Counter()
_prev_counters: Counter[str] = Counter()


def _label_key(name: str, labels: Optional[dict[str, str]]) -> str:
    if not labels:
        return name
    parts = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return f"{name}{{{parts}}}"


def inc(name: str, *, labels: Optional[dict[str, str]] = None, value: int = 1) -> None:
    key = _label_key(name, labels)
    _counters[key] += value


def emit_metrics() -> None:
    changed = False
    for k, v in _counters.items():
        if _prev_counters.get(k) != v:
            logger.info(
                "Metric %s=%s",
                k,
                v,
                extra={"event": "metric", "metric": k, "value": v},
            )
            changed = True
    if changed:
        _prev_counters.clear()
        _prev_counters.update(_counters)


def start(interval: int | None = None) -> Callable[[], None]:
    """Start a background metrics logger. Returns a stop callback."""

    if interval is None:
        interval = _interval
    if interval <= 0:
        return lambda: None
    stop = threading.Event()

    def run() -> None:
        while not stop.wait(interval):
            emit_metrics()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop.set


# Convenience wrappers


def inc_rate_limited() -> None:
    inc("rate_limited_total")


def inc_breaker_open(dep: str) -> None:
    inc("breaker_open_total", labels={"dep": dep})


def inc_api_5xx() -> None:
    inc("api_5xx_total")


def inc_nzb_cache_hit() -> None:
    inc("nzb_cache_hit_total")


def inc_nzb_cache_miss() -> None:
    inc("nzb_cache_miss_total")
