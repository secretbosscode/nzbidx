from __future__ import annotations

# ruff: noqa: E402 - path manipulation before imports

import socket
import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest import nntp_client  # type: ignore


def test_list_groups_sends_auth(monkeypatch) -> None:
    called: dict[str, object] = {}

    class DummyServer:
        def __init__(
            self, host, port=119, user=None, password=None
        ):  # pragma: no cover - trivial
            called["args"] = (host, port, user, password)

        def __enter__(self):  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            return None

        def list(self, pattern=None):  # pragma: no cover - simple
            called["pattern"] = pattern
            return "", [("alt.binaries.example", "0", "0", "0")]

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_USER", "user")
    monkeypatch.setenv("NNTP_PASS", "pass")
    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    groups = client.list_groups()

    assert groups == ["alt.binaries.example"]
    assert called["args"] == ("example.com", 119, "user", "pass")
    assert called["pattern"] == "alt.binaries.*"


def test_high_water_mark_auth(monkeypatch) -> None:
    sent: list[bytes] = []

    class DummySock:
        def __init__(self) -> None:
            self.lines = [
                b"200 welcome\r\n",
                b"200 mode ok\r\n",
                b"381 pass?\r\n",
                b"281 ok\r\n",
                b"211 1 1 2 alt.binaries.example\r\n",
            ]

        def sendall(self, data: bytes) -> None:
            sent.append(data)

        def recv(self, _n: int) -> bytes:
            return self.lines.pop(0) if self.lines else b""

        def __enter__(self):  # pragma: no cover - trivial
            return self

        def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
            pass

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_USER", "user")
    monkeypatch.setenv("NNTP_PASS", "pass")
    monkeypatch.setattr(socket, "create_connection", lambda *_a, **_k: DummySock())

    client = nntp_client.NNTPClient()
    high = client.high_water_mark("alt.binaries.example")

    assert high == 2
    assert any(b"AUTHINFO USER user" in s for s in sent)
    assert any(b"AUTHINFO PASS pass" in s for s in sent)
