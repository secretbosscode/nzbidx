import pytest
from types import SimpleNamespace

from nzbidx_api import main, db


@pytest.mark.asyncio
async def test_search_endpoint_no_runtime_error(monkeypatch):
    await db.init_engine()

    async def fake_search_releases_async(*args, **kwargs):
        return []

    monkeypatch.setattr(main, "search_releases_async", fake_search_releases_async)
    req = SimpleNamespace(query_params={"t": "search"}, headers={})
    try:
        resp = await main.api(req)
    except RuntimeError as exc:  # pragma: no cover - defensive
        pytest.fail(f"RuntimeError raised: {exc}")
    finally:
        await db.dispose_engine()
    assert resp.status_code == 200
