"""Minimal NNTP client used by the ingest worker."""

from __future__ import annotations

import logging
import os
from typing import Optional

from nzbidx_api import config

try:  # pragma: no cover - nntplib is standard library but allow overriding
    import nntplib  # type: ignore
except Exception:  # pragma: no cover - extremely unlikely
    nntplib = None  # type: ignore

logger = logging.getLogger(__name__)


class NNTPClient:
    """Very small NNTP client with a persistent connection."""

    def __init__(self) -> None:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        self.host: Optional[str] = host
        self.port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
        ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
        self.use_ssl = (ssl_env == "1") if ssl_env is not None else self.port == 563
        self.user = os.getenv("NNTP_USER")
        self.password = os.getenv("NNTP_PASS")
        # Default to a generous timeout to handle slow or flaky providers
        self.timeout = float(config.nntp_timeout_seconds())
        self._server: Optional[nntplib.NNTP] = None

    # ------------------------------------------------------------------
    # Connection helpers
    def _create_server(self) -> nntplib.NNTP:
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

    def connect(self) -> None:
        """Establish the persistent NNTP connection."""
        if not self.host:
            logger.info("dry-run: no NNTP providers configured")
            return
        try:
            self._close()
            self._ensure_connection()
        except Exception as exc:  # pragma: no cover - network failure
            logger.warning(
                "connection_failed", extra={"host": self.host, "error": str(exc)}
            )

    def quit(self) -> None:
        """Terminate the connection gracefully."""
        self._close()

    def _reconnect(self) -> None:
        self._close()
        self._ensure_connection()

    # ------------------------------------------------------------------
    # NNTP commands
    def group(self, name: str):
        """Select ``name`` and return the server response."""
        last_exc: Exception | None = None
        for _ in range(2):
            try:
                server = self._ensure_connection()
                if server is None:
                    return "", 0, "0", "0", name
                return server.group(name)
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
                server.group(group)
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
                _resp, _num, _mid, lines = server.body(message_id, decode=False)
                return sum(len(line) for line in lines)
            except Exception:  # pragma: no cover - network failure
                try:
                    self._reconnect()
                except Exception:
                    return 0
        return 0

    # ------------------------------------------------------------------
    def list_groups(self) -> list[str]:
        """Return a list of available NNTP groups."""
        if not self.host:
            return []
        try:
            server = self._create_server()
            with server:
                _resp, groups = server.list("alt.binaries.*")
                return [name for name, *_rest in groups]
        except Exception:  # pragma: no cover - network failure
            return []
