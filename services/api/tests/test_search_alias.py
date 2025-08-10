from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.search import search_releases
from nzbidx_common.os import OS_RELEASES_ALIAS


class DummyOS:
    def __init__(self) -> None:
        self.kwargs = None

    def search(self, **kwargs):  # pragma: no cover - simple stub
        self.kwargs = kwargs
        return {"hits": {"hits": []}}


def test_search_uses_alias():
    client = DummyOS()
    search_releases(client, {"must": []}, limit=1)
    assert client.kwargs["index"] == OS_RELEASES_ALIAS
