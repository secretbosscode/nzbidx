from pathlib import Path
import sys
from contextlib import nullcontext

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))
sys.path.append(str(REPO_ROOT / "services" / "ingest" / "src"))

from nzbidx_api import search as search_mod  # type: ignore  # noqa: E402
from nzbidx_ingest.main import _infer_category, CATEGORY_MAP  # type: ignore  # noqa: E402


def test_basic_api_and_ingest(monkeypatch):
    # Ingest: category inference
    assert _infer_category("Awesome Film [movies]") == CATEGORY_MAP["movies"]

    # API: verify search applies sort
    body_holder: dict[str, object] = {}

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
