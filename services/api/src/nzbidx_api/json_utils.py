"""Utilities for selecting a JSON implementation.

The API prefers the standard library's :mod:`json` module unless the
``NZBIDX_USE_STD_JSON`` environment variable is explicitly set to ``"0"``.
When enabled and available, :mod:`orjson` will be imported.  If it cannot be
imported, a small shim providing compatible ``dumps``/``loads`` functions is
returned instead.  This mirrors the logic previously duplicated across the
codebase.
"""

from __future__ import annotations

import json
import os
from typing import Any

__all__ = ["orjson", "get_json_module"]


class _StdJson:
    """Emulate :mod:`orjson` using the standard library."""

    __slots__ = ()

    @staticmethod
    def dumps(obj: Any, *, option: Any = None, **kw: Any) -> bytes:
        return json.dumps(obj, **kw).encode()

    @staticmethod
    def loads(s: Any, **kw: Any) -> Any:
        return json.loads(s.decode() if isinstance(s, (bytes, bytearray)) else s, **kw)


_STDLIB_JSON = _StdJson()


def get_json_module() -> Any:
    """Return the JSON implementation to use.

    When ``NZBIDX_USE_STD_JSON`` is not ``"0"`` the standard library ``json``
    module is wrapped to emulate the :mod:`orjson` interface.  If the variable
    is set to ``"0"`` the function attempts to import :mod:`orjson`, falling
    back to the same wrapper if the import fails.
    """

    if os.getenv("NZBIDX_USE_STD_JSON", "1") != "0":
        return _STDLIB_JSON

    try:  # pragma: no cover - optional dependency
        import orjson  # type: ignore
    except Exception:  # pragma: no cover - fallback when orjson is absent
        return _STDLIB_JSON
    else:
        return orjson


# Expose the selected module at import time for convenience
orjson = get_json_module()
