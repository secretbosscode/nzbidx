"""Monitor container resources and log anomalies."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_CGROUP_ROOT = Path("/sys/fs/cgroup")

_MEMORY_CURRENT_FILE = _CGROUP_ROOT / "memory.current"
if _MEMORY_CURRENT_FILE.exists():
    _USED_PATH = _MEMORY_CURRENT_FILE
    _LIMIT_PATH = _CGROUP_ROOT / "memory.max"
else:
    _USED_PATH = _CGROUP_ROOT / "memory.usage_in_bytes"
    _LIMIT_PATH = _CGROUP_ROOT / "memory.limit_in_bytes"


def _read(path: Path) -> Optional[int]:
    try:
        return int(path.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def get_memory_stats(
    root: Optional[Path] = None,
) -> Tuple[Optional[int], Optional[int]]:
    """Return memory usage and limit from cgroup files.

    ``root`` optionally overrides the directory containing the cgroup files
    determined at import time.  Returns ``(used, limit)`` in bytes. ``limit``
    is ``None`` if unlimited or unavailable.
    """

    if root is None:
        used_path, limit_path = _USED_PATH, _LIMIT_PATH
    else:
        used_path, limit_path = root / _USED_PATH.name, root / _LIMIT_PATH.name
    used = _read(used_path)
    limit = _read(limit_path)
    if limit is not None and limit > 1 << 60:  # treat very large values as unlimited
        limit = None
    return used, limit


def _monitor(interval: int, root: Path, stop: threading.Event) -> None:
    while not stop.is_set():
        used, limit = get_memory_stats(root)
        if used is not None:
            extra = {"used": used}
            if limit:
                pct = int(used / limit * 100)
                extra.update({"limit": limit, "pct": pct})
                if pct >= 90:
                    logger.warning(
                        "High memory usage: %d of %d bytes (%d%%)",
                        used,
                        limit,
                        pct,
                        extra={"event": "memory_usage_high", **extra},
                    )
                else:
                    logger.info(
                        "Memory usage: %d of %d bytes (%d%%)",
                        used,
                        limit,
                        pct,
                        extra={"event": "memory_usage", **extra},
                    )
            else:
                logger.info(
                    "Memory usage: %d bytes (limit unknown)",
                    used,
                    extra={"event": "memory_usage", **extra},
                )
        if stop.wait(interval):
            break


def start_memory_logger(
    interval: int = 60, root: Path = _CGROUP_ROOT
) -> threading.Event:
    """Start a background thread that logs memory usage periodically.

    Returns the :class:`threading.Event` used to signal the thread to exit."""

    stop = threading.Event()
    thread = threading.Thread(target=_monitor, args=(interval, root, stop), daemon=True)
    thread.start()
    return stop


def install_signal_handlers() -> None:
    """Log termination signals for post-mortem analysis."""
    import signal

    def _handler(signum, _frame):  # pragma: no cover - OS-level signals
        try:
            logger.warning("signal_received", extra={"signal": signum})
        finally:
            raise SystemExit(0)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handler)
