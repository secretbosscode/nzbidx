from __future__ import annotations

from nzbidx_api.json_utils import orjson
import os
import time
from starlette.testclient import TestClient


def _parse(resp):
    return resp.json() if hasattr(resp, "json") else orjson.loads(resp.body)


def test_admin_backfill(monkeypatch) -> None:
    """Backfill endpoint starts job and reports completion."""

    def dummy(progress_cb=None):
        if progress_cb:
            progress_cb(5)
        time.sleep(0.05)
        return 5

    os.environ.setdefault("INGEST_STALE_SECONDS", "5")
    from nzbidx_api import main

    monkeypatch.setattr(main, "backfill_release_parts", dummy)
    with TestClient(main.app) as client:
        resp = client.post("/api/admin/backfill", headers={"X-Api-Key": "secret"})
        assert resp.status_code == 200
        assert _parse(resp)["status"] == "started"
        # poll until complete
        for _ in range(10):
            resp2 = client.post("/api/admin/backfill", headers={"X-Api-Key": "secret"})
            data = _parse(resp2)
            if data["status"] == "complete":
                assert data["processed"] == 5
                break
            time.sleep(0.05)
        else:
            assert False, "backfill did not complete"
