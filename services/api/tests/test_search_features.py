from pathlib import Path
import sys
from contextlib import nullcontext

# Ensure local packages importable
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_api.main import _os_search  # type: ignore  # noqa: E402
from nzbidx_api import search as search_mod  # noqa: E402


def test_os_search_builds_tag_filters_and_fuzzy(monkeypatch):
    captured = {}

    def fake_search_releases(client, query, *, limit, offset=0, sort=None):
        captured["query"] = query
        captured["sort"] = sort
        return []

    monkeypatch.setattr("nzbidx_api.main.search_releases", fake_search_releases)
    monkeypatch.setattr("nzbidx_api.main.opensearch", object())

    _os_search("Metallica", extra={"artist": "Metallica"}, sort="date")

    assert captured["sort"] == "date"
    must = captured["query"].get("must", [])
    assert {"term": {"tags": "metallica"}} in must
    norm_match = must[0]["match"]["norm_title"]
    assert norm_match["fuzziness"] == "AUTO"


def test_search_releases_applies_sort(monkeypatch):
    body_holder = {}

    class DummyClient:
        def search(self, **kwargs):
            body_holder["body"] = kwargs["body"]
            return {"hits": {"hits": []}}

    def dummy_call_with_retry(breaker, dep, func, **kwargs):
        return func(**kwargs)

    monkeypatch.setattr(search_mod, "call_with_retry", dummy_call_with_retry)
    monkeypatch.setattr(search_mod, "start_span", lambda name: nullcontext())

    client = DummyClient()
    search_mod.search_releases(client, {"must": []}, limit=5, sort="date")

    assert body_holder["body"]["sort"] == [{"posted_at": {"order": "desc"}}]
