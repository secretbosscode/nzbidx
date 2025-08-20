from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from nzbidx_ingest import nntp_client  # type: ignore


def test_list_groups_sends_auth(monkeypatch) -> None:
    called: dict[str, object] = {}

    class DummyServer:
        def __init__(
            self, host, port=119, user=None, password=None, timeout=None
        ):  # pragma: no cover - trivial
            called["args"] = (host, port, user, password, timeout)

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
    assert called["args"] == ("example.com", 119, "user", "pass", 30.0)
    assert called["pattern"] == "alt.binaries.*"


def test_high_water_mark_auth(monkeypatch) -> None:
    called: dict[str, object] = {}

    class DummyServer:
        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            called["args"] = (host, port, user, password, timeout)

        def reader(self) -> None:  # pragma: no cover - trivial
            pass

        def quit(self) -> None:  # pragma: no cover - trivial
            pass

        def group(self, group: str):  # pragma: no cover - simple
            called["group"] = group
            return "", 0, "1", "2", group

    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_USER", "user")
    monkeypatch.setenv("NNTP_PASS", "pass")
    monkeypatch.setattr(nntp_client.config, "nntp_timeout_seconds", lambda: 60)
    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    high = client.high_water_mark("alt.binaries.example")

    assert high == 2
    assert called["args"] == ("example.com", 119, "user", "pass", 60.0)
    assert called["group"] == "alt.binaries.example"


def test_high_water_mark_reconnect(monkeypatch) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")

    class DummyServer:
        instances = 0
        fail_next = True

        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            DummyServer.instances += 1

        def reader(self) -> None:  # pragma: no cover - trivial
            pass

        def quit(self) -> None:  # pragma: no cover - trivial
            pass

        def group(self, group: str):  # pragma: no cover - simple
            if DummyServer.fail_next:
                DummyServer.fail_next = False
                raise OSError("connection dropped")
            return "", 0, "1", "2", group

    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    high = client.high_water_mark("alt.binaries.example")

    assert high == 2
    assert DummyServer.instances == 2


def test_quit_closes_connection(monkeypatch) -> None:
    monkeypatch.setenv("NNTP_HOST", "example.com")

    called: dict[str, int] = {"quit": 0}

    class DummyServer:
        def __init__(self, host, port=119, user=None, password=None, timeout=None):
            pass

        def reader(self) -> None:  # pragma: no cover - trivial
            pass

        def quit(self) -> None:  # pragma: no cover - trivial
            called["quit"] += 1

    monkeypatch.setattr(
        nntp_client,
        "nntplib",
        SimpleNamespace(NNTP=DummyServer, NNTP_SSL=DummyServer, NNTP_SSL_PORT=563),
    )

    client = nntp_client.NNTPClient()
    client.connect()
    client.quit()

    assert called["quit"] == 1


def test_missing_nntplib_raises(monkeypatch) -> None:
    """Raise ``RuntimeError`` when no NNTP implementation is available."""

    # Simulate absence of the stdlib ``nntplib``
    monkeypatch.setitem(sys.modules, "nntplib", None)

    import importlib

    # Reload compatibility layer and client to pick up the change
    import nzbidx_ingest.nntp_compat as nntp_compat

    importlib.reload(nntp_compat)
    import nzbidx_ingest.nntp_client as nntp_client_reload

    importlib.reload(nntp_client_reload)

    monkeypatch.setenv("NNTP_HOST", "example.com")

    client = nntp_client_reload.NNTPClient()
    with pytest.raises(RuntimeError):
        client._create_server()

    # Restore original modules for other tests
    importlib.reload(nntp_compat)
    importlib.reload(nntp_client)
