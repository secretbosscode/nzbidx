"""Monitor container resources and log anomalies."""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_CGROUP_ROOT = Path("/sys/fs/cgroup")


_MEMORY_USED_FD: Optional[int] = None
_MEMORY_LIMIT_FD: Optional[int] = None
_MEMORY_ROOT: Optional[Path] = None


def _open_memory_files(root: Path) -> None:
    """Open cgroup memory usage and limit files and keep their descriptors."""
    global _MEMORY_USED_FD, _MEMORY_LIMIT_FD, _MEMORY_ROOT

    for fd in (_MEMORY_USED_FD, _MEMORY_LIMIT_FD):
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    _MEMORY_USED_FD = _MEMORY_LIMIT_FD = None

    try:
        _MEMORY_USED_FD = os.open(root / "memory.current", os.O_RDONLY | os.O_CLOEXEC)
    except FileNotFoundError:
        try:
            _MEMORY_USED_FD = os.open(
                root / "memory.usage_in_bytes", os.O_RDONLY | os.O_CLOEXEC
            )
        except FileNotFoundError:
            _MEMORY_USED_FD = None
        try:
            _MEMORY_LIMIT_FD = os.open(
                root / "memory.limit_in_bytes", os.O_RDONLY | os.O_CLOEXEC
            )
        except FileNotFoundError:
            _MEMORY_LIMIT_FD = None
    else:
        try:
            _MEMORY_LIMIT_FD = os.open(root / "memory.max", os.O_RDONLY | os.O_CLOEXEC)
        except FileNotFoundError:
            _MEMORY_LIMIT_FD = None

    _MEMORY_ROOT = root


def _read_fd(fd: Optional[int], *, is_used: bool) -> Optional[int]:
    global _MEMORY_USED_FD, _MEMORY_LIMIT_FD
    if fd is None:
        return None
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, 32)
        return int(data.strip())
    except (OSError, ValueError):
        try:
            os.close(fd)
        finally:
            if is_used:
                _MEMORY_USED_FD = None
            else:
                _MEMORY_LIMIT_FD = None
        return None


def get_memory_stats(root: Path = _CGROUP_ROOT) -> Tuple[Optional[int], Optional[int]]:
    """Return memory usage and limit from cgroup files.

    ``root`` is the cgroup directory.  The function detects both cgroup v2
    (``memory.current``/``memory.max``) and v1
    (``memory.usage_in_bytes``/``memory.limit_in_bytes``).  The relevant files
    are opened once and reused between calls.  Returns ``(used, limit)`` in
    bytes. ``limit`` is ``None`` if unlimited or unavailable.
    """

    global _MEMORY_ROOT
    if root != _MEMORY_ROOT:
        _open_memory_files(root)

    used = _read_fd(_MEMORY_USED_FD, is_used=True)
    limit = _read_fd(_MEMORY_LIMIT_FD, is_used=False)
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
