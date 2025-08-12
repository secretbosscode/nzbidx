import os
import sys
import importlib

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from nzbidx_ingest import config, nntp_client


def test_auto_discovers_groups(monkeypatch):
    monkeypatch.delenv("NNTP_GROUPS", raising=False)

    def fake_list_groups(self):
        return ["alt.binaries.movies", "alt.binaries.tv"]

    monkeypatch.setattr(nntp_client.NNTPClient, "list_groups", fake_list_groups)
    importlib.reload(config)
    assert config.NNTP_GROUPS == ["alt.binaries.movies", "alt.binaries.tv"]
