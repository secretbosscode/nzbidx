"""General utility helpers for the API service."""

from __future__ import annotations

import inspect
from typing import Any


async def maybe_await(value: Any) -> Any:
    """Return the result of ``value``, awaiting it if necessary."""
    if inspect.isawaitable(value):
        return await value
    return value
