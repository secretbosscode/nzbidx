"""Minimal NNTP client used by the ingest worker."""

from __future__ import annotations

import logging
import os
import socket
import ssl

try:  # pragma: no cover - nntplib is standard library but allow overriding
    import nntplib  # type: ignore
except Exception:  # pragma: no cover - extremely unlikely
    nntplib = None  # type: ignore

logger = logging.getLogger(__name__)


class NNTPClient:
    """Very small NNTP client that performs a simple connection test."""

    def connect(self) -> None:
        """Connect to the NNTP provider specified via environment variables."""
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        if not host:
            logger.info("dry-run: no NNTP providers configured")
            return

        port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
        ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
        use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563

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

    def list_groups(self) -> list[str]:
        """Return a list of available NNTP groups."""
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        if not host:
            return []

        port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
        ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
        use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563

        try:
            cls = nntplib.NNTP_SSL if use_ssl else nntplib.NNTP
            with cls(
                host,
                port=port,
                user=os.getenv("NNTP_USER"),
                password=os.getenv("NNTP_PASS"),
            ) as server:
                _resp, groups = server.list()
                return [name for name, *_rest in groups]
        except Exception:  # pragma: no cover - network failure
            return []

    def _auth_if_needed(self, sock: socket.socket) -> None:
        user = os.getenv("NNTP_USER")
        password = os.getenv("NNTP_PASS")
        if not user or not password:
            return
        try:
            sock.sendall(f"AUTHINFO USER {user}\r\n".encode())
            self._recv_line(sock)
            sock.sendall(f"AUTHINFO PASS {password}\r\n".encode())
            self._recv_line(sock)
        except Exception:
            pass

    # The real client would issue commands like ``GROUP`` to discover the high
    # water mark for a group.  The simplified test environment has no NNTP
    # server, so this method returns ``0`` when no provider is configured.  When
    # connection details are present it performs a minimal ``GROUP`` command to
    # obtain the highest article number.
    def high_water_mark(self, group: str) -> int:
        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        if not host:
            return 0

        port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
        ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
        use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563

        try:
            if use_ssl:
                context = ssl.create_default_context()
                with socket.create_connection((host, port), timeout=10) as sock:
                    with context.wrap_socket(sock, server_hostname=host) as ssock:
                        return self._group_high(ssock, group)
            else:
                with socket.create_connection((host, port), timeout=10) as sock:
                    return self._group_high(sock, group)
        except Exception:  # pragma: no cover - network failure
            return 0

    def _group_high(self, sock: socket.socket, group: str) -> int:
        # Read greeting line
        try:
            self._recv_line(sock)
            sock.sendall(b"MODE READER\r\n")
            self._recv_line(sock)
            self._auth_if_needed(sock)
            sock.sendall(f"GROUP {group}\r\n".encode())
            response = self._recv_line(sock)
        except Exception:  # pragma: no cover - network issues
            return 0

        parts = response.split()
        if len(parts) >= 4:
            try:
                return int(parts[3])
            except ValueError:  # pragma: no cover - malformed response
                return 0
        return 0

    # The previous implementation returned an empty list which meant the ingest
    # worker never received any headers to process when running against a real
    # NNTP server.  Implement a minimal ``XOVER`` helper so that the worker can
    # pull overview information and persist releases to the database.
    def xover(self, group: str, start: int, end: int) -> list[dict[str, object]]:
        """Return header dicts for articles in ``group`` between ``start`` and ``end``."""

        host = os.getenv("NNTP_HOST_1") or os.getenv("NNTP_HOST")
        if not host:
            return []

        port = int(os.getenv("NNTP_PORT_1") or os.getenv("NNTP_PORT") or "119")
        ssl_env = os.getenv("NNTP_SSL_1") or os.getenv("NNTP_SSL")
        use_ssl = (ssl_env == "1") if ssl_env is not None else port == 563

        try:
            cls = nntplib.NNTP_SSL if use_ssl else nntplib.NNTP
            with cls(
                host,
                port=port,
                user=os.getenv("NNTP_USER"),
                password=os.getenv("NNTP_PASS"),
            ) as server:
                server.group(group)
                _resp, overviews = server.xover(start, end)
                # ``nntplib`` returns a list of ``(id, header_dict)`` tuples
                # while our ingest loop expects just the header dictionaries.
                # Some test doubles may already provide dictionaries, so handle
                # both shapes here.
                return [ov[1] if isinstance(ov, tuple) else ov for ov in overviews]
        except Exception:  # pragma: no cover - network failure
            return []
