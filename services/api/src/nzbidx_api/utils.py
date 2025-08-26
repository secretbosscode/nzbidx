"""General utility helpers for the API service."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any


async def maybe_await(value: Any) -> Any:
    """Return the result of ``value``, awaiting it if necessary."""
    if isinstance(value, Awaitable):
        return await value
    return value
