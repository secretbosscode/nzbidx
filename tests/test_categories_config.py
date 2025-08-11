import importlib
import json
import sys
from pathlib import Path


def test_caps_xml_uses_config(tmp_path, monkeypatch):
    cfg = tmp_path / "cats.json"
    cfg.write_text(
        json.dumps(
            [
                {"id": 123, "name": "Foo"},
                {"id": 6000, "name": "Adult"},
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CATEGORY_CONFIG", str(cfg))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/api/src"))
    newznab = importlib.import_module("nzbidx_api.newznab")
    newznab = importlib.reload(newznab)
    xml = newznab.caps_xml()
    assert '<category id="123" name="Foo"/>' in xml
    assert '<category id="6000"' not in xml
