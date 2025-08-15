from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path

# ruff: noqa: E402 - path manipulation before imports
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_includes_apikey(monkeypatch) -> None:
    class DummyClient:
        def search(self, **kwargs):
            return {
                "hits": {
                    "hits": [
                        {
                            "_id": "1",
                            "_source": {
                                "norm_title": "",
                                "posted_at": "",
                                "category": "",
                                "size_bytes": 1,
                            },
                        }
                    ]
                }
            }

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    items = search_mod.search_releases(client, {"must": []}, limit=1, api_key="secret")
    assert items[0]["link"].endswith("apikey=secret")
