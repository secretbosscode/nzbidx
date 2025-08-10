"""Minimal NNTP client used by the ingest service."""

from __future__ import annotations

import logging
import os
import socket
import ssl

logger = logging.getLogger(__name__)


class NNTPClient:
    """Very small NNTP client that performs a simple connection test."""

    def connect(self) -> None:
        """Connect to the NNTP provider specified via environment variables."""
        host = os.getenv("NNTP_HOST_1")
        if not host:
            logger.info("dry-run: no NNTP providers configured")
            return

        port = int(os.getenv("NNTP_PORT_1", "119"))
        use_ssl = os.getenv("NNTP_SSL_1") == "1"

        try:
            if use_ssl:
                context = ssl.create_default_context()
                with socket.create_connection((host, port), timeout=10) as sock:
                    with context.wrap_socket(sock, server_hostname=host) as ssock:
                        self._talk(ssock, host, port)
            else:
                with socket.create_connection((host, port), timeout=10) as sock:
                    self._talk(sock, host, port)
        except Exception as exc:  # pragma: no cover - network failure
            logger.warning("connection_failed", extra={"host": host, "error": str(exc)})

    def _talk(self, sock: socket.socket, host: str, port: int) -> None:
        """Exchange a minimal greeting with the NNTP server."""
        greeting = self._recv_line(sock)
        logger.info(
            "nntp_greeting",
            extra={"host": host, "port": port, "greeting": greeting},
        )
        try:
            sock.sendall(b"MODE READER\r\n")
            response = self._recv_line(sock)
            logger.info("mode_reader", extra={"response": response})
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("command_failed", extra={"error": str(exc)})

    @staticmethod
    def _recv_line(sock: socket.socket) -> str:
        data = sock.recv(1024)
        return data.decode(errors="ignore").strip()

    # The real client would issue ``XOVER`` or ``HDR`` commands.  For the
    # minimal test environment this method simply returns an empty list and is
    # monkeypatched in tests.
    def xover(self, group: str, start: int, end: int) -> list[dict[str, object]]:
        """Return header dicts for articles in ``group`` between ``start`` and ``end``."""
        return []
