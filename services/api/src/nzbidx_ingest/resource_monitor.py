"""Monitor container resources and log anomalies."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_CGROUP_ROOT = Path("/sys/fs/cgroup")


def _read(path: Path) -> Optional[int]:
    try:
        return int(path.read_bytes())
    except (FileNotFoundError, ValueError):
        return None


def get_memory_stats(root: Path = _CGROUP_ROOT) -> Tuple[Optional[int], Optional[int]]:
    """Return memory usage and limit from cgroup files.

    ``root`` is the cgroup directory.  The function detects both cgroup v2
    (``memory.current``/``memory.max``) and v1
    (``memory.usage_in_bytes``/``memory.limit_in_bytes``).
    Returns ``(used, limit)`` in bytes. ``limit`` is ``None`` if unlimited or
    unavailable.
    """

    used = _read(root / "memory.current")
    limit = _read(root / "memory.max")
    if used is None:
        used = _read(root / "memory.usage_in_bytes")
        limit = _read(root / "memory.limit_in_bytes")
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

    monitored_signal_names = (
        "SIGTERM",
        "SIGINT",
        "SIGQUIT",
        "SIGUSR1",
        "SIGUSR2",
    )

    for name in monitored_signal_names:
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handler)
        except (OSError, RuntimeError, ValueError):
            logger.debug("unable_to_install_signal_handler", extra={"signal": name})

    try:  # Optional enhanced diagnostics for fatal signals
        import faulthandler
    except Exception:  # pragma: no cover - best effort setup
        return

    try:
        if not faulthandler.is_enabled():
            faulthandler.enable()
    except (OSError, RuntimeError, ValueError):  # pragma: no cover - depends on env
        logger.debug("unable_to_enable_faulthandler")
        return

    fatal_signal_names = ("SIGSEGV", "SIGFPE", "SIGABRT", "SIGBUS", "SIGILL")
    for name in fatal_signal_names:
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            faulthandler.register(sig, chain=True)
        except (ValueError, OSError, RuntimeError):  # pragma: no cover - env dependent
            logger.debug("unable_to_register_faulthandler", extra={"signal": name})
