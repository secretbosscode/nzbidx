"""Policy related tests."""

from pathlib import Path
import sys

# Ensure the application package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import nzbidx_api.main as main  # noqa: E402


class DummyOS:
    def __init__(self) -> None:
        self.body = None

    def search(self, *, index: str, body: dict):
        self.body = body
        return {"hits": {"hits": []}}


def test_xxx_disabled(monkeypatch) -> None:
    dummy = DummyOS()
    old = main.opensearch
    main.opensearch = dummy
    monkeypatch.setenv("ALLOW_XXX", "false")
    main._os_search("test")
    assert {"term": {"category": "xxx"}} in dummy.body["query"]["bool"].get(
        "must_not", []
    )
    main.opensearch = old
