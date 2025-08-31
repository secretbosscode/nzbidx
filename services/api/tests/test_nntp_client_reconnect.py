from __future__ import annotations

import time
from types import SimpleNamespace

from nzbidx_ingest import config as ingest_config, nntp_client  # type: ignore


def test_reconnect_after_outage(monkeypatch) -> None:
    """Client should reconnect after extended outage once connectivity returns."""

    class FlakyServer:
        attempts = 0
        fail_for = 5

        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            FlakyServer.attempts += 1
            if FlakyServer.attempts <= FlakyServer.fail_for:
                raise OSError("unreachable")

        def reader(self) -> None:  # pragma: no cover - trivial
            pass

        def quit(self) -> None:  # pragma: no cover - trivial
            pass

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_CONNECT_BASE", "0.01")
    monkeypatch.setenv("NNTP_CONNECT_MAX_DELAY", "0.02")
    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=FlakyServer, NNTP_SSL=FlakyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient(ingest_config.nntp_settings())

    # Initial connection should fail and start background retries
    assert client.connect() is False

    # Wait for background thread to establish connection
    deadline = time.time() + 1.0
    while client._server is None and time.time() < deadline:
        time.sleep(0.01)

    assert client._server is not None
    assert FlakyServer.attempts > FlakyServer.fail_for
    client.quit()


def test_quit_stops_retry_thread(monkeypatch) -> None:
    """Background reconnect thread should terminate when quitting."""

    class FailingServer:
        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            raise OSError("unreachable")

        def reader(self) -> None:  # pragma: no cover - trivial
            pass

        def quit(self) -> None:  # pragma: no cover - trivial
            pass

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_CONNECT_BASE", "0.01")
    monkeypatch.setenv("NNTP_CONNECT_MAX_DELAY", "0.02")
    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=FailingServer, NNTP_SSL=FailingServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient(ingest_config.nntp_settings())

    assert client.connect() is False
    thread = client._connect_thread
    assert thread is not None
    deadline = time.time() + 1.0
    while not thread.is_alive() and time.time() < deadline:
        time.sleep(0.01)
    assert thread.is_alive()
    client.quit()
    assert client._connect_thread is None
    assert thread.is_alive() is False
