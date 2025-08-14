import json
from typing import Any

# Minimal stub of the orjson interface used in tests.
# Provides dumps and loads functions compatible with orjson.


def dumps(
    obj: Any, *args: Any, **kwargs: Any
) -> bytes:  # pragma: no cover - thin wrapper
    """Serialize ``obj`` to JSON and return bytes.

    This stub mirrors ``orjson.dumps`` by returning UTF-8 encoded bytes.  The
    implementation delegates to the standard library ``json`` module, ignoring
    any additional options that ``orjson`` would normally support.
    """
    return json.dumps(obj).encode("utf-8")


def loads(
    data: Any, *args: Any, **kwargs: Any
) -> Any:  # pragma: no cover - thin wrapper
    """Deserialize ``data`` (bytes or str) to a Python object."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return json.loads(data)
