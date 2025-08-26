"""Integration tests exercising nzbidx's external dependencies."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import time
from types import SimpleNamespace

os.environ["API_KEYS"] = "secret"
os.environ["INGEST_STALE_SECONDS"] = "5"

import nzbidx_api.main as main  # type: ignore  # noqa: E402


try:  # pragma: no cover - prefer real TestClient when available
    from starlette.testclient import TestClient  # type: ignore
except ModuleNotFoundError:  # pragma: no cover

    class TestClient:
        """Very small subset of Starlette's TestClient for dependency tests."""

        def __init__(self, app: object) -> None:
            self.app = app

        def __enter__(self) -> "TestClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, path: str, params: dict | None = None):  # type: ignore[override]
            request = SimpleNamespace(query_params=params or {}, headers={}, url=None)
            for route in getattr(self.app, "routes", []):
                if getattr(route, "path", None) == path:
                    resp = route.endpoint(request)
                    if inspect.iscoroutine(resp):
                        resp = asyncio.run(resp)
                    return resp
            return SimpleNamespace(status_code=404, body=b"", headers={})

        def post(
            self, path: str, json: dict | None = None, headers: dict | None = None
        ):  # type: ignore[override]
            async def _json() -> dict:
                return json or {}

            request = SimpleNamespace(
                query_params={}, headers=headers or {}, json=_json, url=None
            )
            for route in getattr(self.app, "routes", []):
                if getattr(route, "path", None) == path:
                    resp = route.endpoint(request)
                    if inspect.iscoroutine(resp):
                        resp = asyncio.run(resp)
                    return resp
            return SimpleNamespace(status_code=404, body=b"", headers={})


app = main.app


def test_health_endpoint() -> None:
    """Basic smoke test for CI to ensure app responds."""
    with TestClient(app) as client:
        response = client.get("/api/health", params={"apikey": "secret"})
        assert response.status_code == 200


def test_health_ingest_warning() -> None:
    """Health endpoint warns when ingest is stale."""
    with TestClient(app) as client:
        # Simulate stale ingest
        main.ingest_loop.last_run = time.monotonic() - 10
        main.ingest_loop.last_run_wall = time.time() - 10
        response = client.get("/api/health", params={"apikey": "secret"})
        if hasattr(response, "json"):
            data = response.json()
        else:
            data = json.loads(response.body)
        assert data["status"] == "warn"
        assert data["ingest"] == "stale"

        # Now simulate recent ingest
        main.ingest_loop.last_run = time.monotonic()
        main.ingest_loop.last_run_wall = time.time()
        response = client.get("/api/health", params={"apikey": "secret"})
        if hasattr(response, "json"):
            data = response.json()
        else:
            data = json.loads(response.body)
        assert data["ingest"] == "ok"


def test_status_endpoint() -> None:
    """Status endpoint exposes dependency and breaker states."""
    with TestClient(app) as client:
        response = client.get("/api/status", params={"apikey": "secret"})
        assert response.status_code == 200
        if hasattr(response, "json"):
            data = response.json()
        else:
            data = json.loads(response.body)
        assert data["breaker"]["os"] == "closed"


def test_config_endpoint(monkeypatch) -> None:
    """Config endpoint exposes effective timeout settings."""
    monkeypatch.setenv("NNTP_TOTAL_TIMEOUT", "77")
    monkeypatch.setenv("NZB_TIMEOUT_SECONDS", "80")
    from nzbidx_api import config as cfg

    cfg.settings.reload()
    with TestClient(app) as client:
        response = client.get("/api/config", params={"apikey": "secret"})
        assert response.status_code == 200
        if hasattr(response, "json"):
            data = response.json()
        else:
            data = json.loads(response.body)
        assert data["nzb_timeout_seconds"] == 80
        assert data["nntp_total_timeout_seconds"] == 77
    cfg.settings.reload()
