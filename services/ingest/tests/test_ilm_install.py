from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from nzbidx_ingest import ilm  # noqa: E402


class DummyClient:
    def __init__(self) -> None:
        self.policy = False
        self.template = False
        self.index = False
        self.calls: list[str] = []
        self.ilm = self
        self.indices = self

    # ILM
    def get_lifecycle(self, name: str):
        if not self.policy:
            raise RuntimeError("missing")
        return {}

    def put_lifecycle(self, name: str, body):
        self.policy = True
        self.calls.append("policy")

    # Templates
    def exists_index_template(self, name: str) -> bool:
        return self.template

    def put_index_template(self, name: str, body):
        self.template = True
        self.calls.append("template")

    # Indices
    def exists(self, index: str) -> bool:
        return self.index

    def create(self, index: str, aliases):
        self.index = True
        self.calls.append("index")

    def get_alias(self, name: str):
        if not self.index:
            raise RuntimeError("missing")
        return {"idx": {"aliases": {name: {"is_write_index": True}}}}

    def put_alias(self, index: str, name: str, is_write_index: bool):
        self.calls.append("alias")


def test_install_idempotent() -> None:
    client = DummyClient()
    ilm.install(client)
    ilm.install(client)
    assert client.calls == ["policy", "template", "index"]
