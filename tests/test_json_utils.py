import importlib
import json
import sys

import pytest


def _reload_json_utils(monkeypatch):
    monkeypatch.delitem(sys.modules, "nzbidx_api.json_utils", raising=False)
    return importlib.import_module("nzbidx_api.json_utils")


@pytest.mark.parametrize("env_val", ["1", "0"])
def test_fallback_to_stdlib_when_orjson_missing(monkeypatch, env_val):
    monkeypatch.setenv("NZBIDX_USE_STD_JSON", env_val)
    monkeypatch.delitem(sys.modules, "orjson", raising=False)
    ju = _reload_json_utils(monkeypatch)
    data = {"a": 1}
    dumped = ju.orjson.dumps(data)
    assert dumped == json.dumps(data).encode()
    assert ju.orjson.loads(dumped) == data


def test_uses_orjson_when_available_and_enabled(monkeypatch):
    class DummyOrjson:
        def __init__(self):
            self.dumps_called = False
            self.loads_called = False

        def dumps(self, obj, *, option=None, **kw):
            self.dumps_called = True
            return b"dummy"

        def loads(self, s, **kw):
            self.loads_called = True
            return {"dummy": True}

    dummy = DummyOrjson()
    monkeypatch.setenv("NZBIDX_USE_STD_JSON", "0")
    monkeypatch.setitem(sys.modules, "orjson", dummy)
    ju = _reload_json_utils(monkeypatch)
    assert ju.orjson is dummy
    ju.orjson.dumps({})
    ju.orjson.loads(b"")
    assert dummy.dumps_called and dummy.loads_called
