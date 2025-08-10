from __future__ import annotations

import uuid
from pathlib import Path
import sys

from starlette.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
import nzbidx_api.main as main  # noqa: E402


def _client(monkeypatch) -> TestClient:
    monkeypatch.delenv("API_KEYS", raising=False)
    import importlib

    importlib.reload(main)
    return TestClient(main.app)


def test_request_id_echo(monkeypatch, capsys) -> None:
    client = _client(monkeypatch)
    resp = client.get("/api?t=caps", headers={"X-Request-ID": "abc"})
    assert resp.headers["X-Request-ID"] == "abc"
    captured = capsys.readouterr().err
    assert '"request_id": "abc"' in captured


def test_request_id_generated(monkeypatch, capsys) -> None:
    client = _client(monkeypatch)
    resp = client.get("/api?t=caps")
    req_id = resp.headers.get("X-Request-ID")
    assert req_id is not None
    uuid.UUID(req_id)
    captured = capsys.readouterr().err
    assert f'"request_id": "{req_id}"' in captured
