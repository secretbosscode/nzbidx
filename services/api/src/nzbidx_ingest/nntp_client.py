"""Minimal NNTP client used by the ingest worker."""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional, TYPE_CHECKING

from nzbidx_api import config

# ``nntplib`` was removed in Python 3.13. Import it via a compatibility layer
# that can rely on the third-party ``standard-nntplib`` package when the
# standard library module is absent.
from .nntp_compat import nntplib

if TYPE_CHECKING:  # pragma: no cover - type hint only
    from .config import NNTPSettings

logger = logging.getLogger(__name__)


class NNTPClient:
    """Very small NNTP client with a persistent connection."""

    def __init__(self, settings: "NNTPSettings") -> None:
        self.host: Optional[str] = settings.host
        self.port = settings.port
        self.use_ssl = settings.use_ssl
        self.user = settings.user
        self.password = settings.password
        self.base = getattr(settings, "base", 1.0)
        self.max_delay = getattr(settings, "max_delay", 60.0)
        # Default to a generous timeout to handle slow or flaky providers
        self.timeout = float(config.nntp_timeout_seconds())
        self._server: Optional[nntplib.NNTP] = None
        self._current_group: Optional[str] = None
        self._connect_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Connection helpers
    def _create_server(self) -> nntplib.NNTP:
        if nntplib is None:  # pragma: no cover - no compatible library
            raise RuntimeError("No NNTP library available")
        cls = nntplib.NNTP_SSL if self.use_ssl else nntplib.NNTP
        return cls(
            self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            timeout=self.timeout,
        )

    def _ensure_connection(self) -> Optional[nntplib.NNTP]:
        if not self.host:
            return None
        if self._server is None:
            self._server = self._create_server()
            try:  # pragma: no cover - depends on server capabilities
                self._server.reader()
            except Exception:
                pass
        return self._server

    def _close(self) -> None:
        if self._server is not None:
            try:
                self._server.quit()
            except Exception:  # pragma: no cover - network failures
                pass
            self._server = None
            self._current_group = None

    def _connect_with_retry(self) -> Optional[nntplib.NNTP]:
        if not self.host:
            return None
        attempt = 0
        while True:
            attempt += 1
            logger.info(
                "connection_attempt", extra={"host": self.host, "attempt": attempt}
            )
            try:
                server = self._ensure_connection()
                self._connect_thread = None
                return server
            except Exception as exc:  # pragma: no cover - network failure
                delay = min(self.max_delay, self.base * 2 ** (attempt - 1))
                logger.warning(
                    "connection_attempt_failed",
                    extra={
                        "host": self.host,
                        "attempt": attempt,
                        "error": str(exc),
                        "delay": delay,
                    },
                )
                time.sleep(delay)

    def connect(self) -> bool:
        """Establish the persistent NNTP connection.

        Returns ``True`` when the connection was established immediately. If
        the initial attempt fails, a background thread is started to retry and
        ``False`` is returned.
        """
        if not self.host:
            logger.info("dry-run: no NNTP providers configured")
            return True
        if self._connect_thread is not None and self._connect_thread.is_alive():
            return False
        self._close()
        try:
            self._ensure_connection()
            return True
        except Exception as exc:  # pragma: no cover - network failure
            logger.warning(
                "connection_failed", extra={"host": self.host, "error": str(exc)}
            )
            self._connect_thread = threading.Thread(
                target=self._connect_with_retry, daemon=True
            )
            self._connect_thread.start()
            return False

    def quit(self) -> None:
        """Terminate the connection gracefully."""
        self._close()

    def _reconnect(self) -> None:
        self.connect()

    # ------------------------------------------------------------------
    # NNTP commands
    def group(self, name: str):
        """Select ``name`` and return the server response."""
        last_exc: Exception | None = None
        self._current_group = None
        for _ in range(2):
            try:
                server = self._ensure_connection()
                if server is None:
                    return "", 0, "0", "0", name
                resp = server.group(name)
                self._current_group = name
                return resp
            except Exception as exc:  # pragma: no cover - network failure
                last_exc = exc
                try:
                    self._reconnect()
                except Exception as reconnect_exc:  # pragma: no cover - network failure
                    logger.warning(
                        "reconnect_failed",
                        extra={"host": self.host, "error": str(reconnect_exc)},
                    )
                    return "", 0, "0", "0", name
        if last_exc:
            logger.warning(
                "group_failed", extra={"group": name, "error": str(last_exc)}
            )
        return "", 0, "0", "0", name

    def high_water_mark(self, group: str) -> int:
        """Return the highest article number for ``group``."""
        if not self.host:
            return 0
        try:
            _resp, _count, _low, high, _name = self.group(group)
            return int(high)
        except Exception:
            return 0

    def xover(self, group: str, start: int, end: int) -> list[dict[str, object]]:
        """Return header dicts for articles in ``group`` between ``start`` and ``end``."""
        if not self.host:
            return []
        for _ in range(2):
            try:
                server = self._ensure_connection()
                if server is None:
                    return []
                if self._current_group != group:
                    server.group(group)
                    self._current_group = group
                _resp, overviews = server.xover(start, end)
                result = []
                for ov in overviews:
                    data = ov[1] if isinstance(ov, tuple) else ov
                    data = dict(data)
                    if ":bytes" in data and "bytes" not in data:
                        # normalize byte count key for downstream consumers
                        data["bytes"] = data.pop(":bytes")
                    elif ":bytes" in data:
                        data.pop(":bytes")
                    result.append(data)
                return result
            except Exception:  # pragma: no cover - network failure
                try:
                    self._reconnect()
                except Exception as reconnect_exc:  # pragma: no cover - network failure
                    logger.warning(
                        "reconnect_failed",
                        extra={"host": self.host, "error": str(reconnect_exc)},
                    )
                    return []
        return []

    # ------------------------------------------------------------------
    def body_size(self, message_id: str) -> int:
        """Return the size in bytes of ``message_id``."""
        if not self.host:
            return 0
        for _ in range(2):
            try:
                server = self._ensure_connection()
                if server is None:
                    return 0

                # Try to parse ``Bytes`` header via ``HEAD`` first
                try:
                    head_resp = server.head(message_id)
                    headers: list[str | bytes]
                    if isinstance(head_resp, tuple):
                        if len(head_resp) == 4:
                            _resp, _num, _mid, headers = head_resp
                        elif len(head_resp) == 2:
                            _resp, info = head_resp
                            headers = getattr(info, "lines", [])
                        else:
                            headers = []
                    else:
                        headers = []
                    for line in headers:
                        text = (
                            line.decode(errors="ignore")
                            if isinstance(line, bytes)
                            else line
                        )
                        if text.lower().startswith("bytes:"):
                            try:
                                return int(text.split(":", 1)[1].strip())
                            except Exception:
                                pass
                except Exception:
                    pass

                # Some servers include the size in the ``STAT`` response
                try:
                    stat_resp = server.stat(message_id)
                    if isinstance(stat_resp, tuple):
                        resp_text = str(stat_resp[0])
                        extra = stat_resp[1:]
                    else:
                        resp_text = str(stat_resp)
                        extra: tuple[object, ...] = ()
                    parts = resp_text.split()
                    if parts and parts[-1].isdigit():
                        return int(parts[-1])
                    for val in extra:
                        if isinstance(val, int):
                            return val
                        if isinstance(val, (bytes, str)) and str(val).isdigit():
                            return int(val)
                except Exception:
                    pass

                # Fall back to fetching the body if other methods fail
                _resp, _num, _mid, lines = server.body(message_id, decode=False)
                return sum(len(line) for line in lines)
            except Exception:  # pragma: no cover - network failure
                try:
                    self._reconnect()
                except Exception:
                    return 0
        return 0

    # ------------------------------------------------------------------
    def list_groups(self, pattern: str = "alt.binaries.*") -> list[str]:
        """Return a list of available NNTP groups."""
        if not self.host:
            return []
        try:
            server = self._create_server()
            with server:
                _resp, groups = server.list(pattern)
                return [name for name, *_rest in groups]
        except Exception:  # pragma: no cover - network failure
            return []
