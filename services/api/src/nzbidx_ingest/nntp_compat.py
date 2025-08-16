"""Compatibility layer for NNTP clients.

Attempt to import the standard library :mod:`nntplib`.  When running on
Python versions where it has been removed (3.13+), fall back to the
`aio-nntp` package if it is available.  The asynchronous client is wrapped
with synchronous helpers so the rest of the code can continue to use the
blocking ``nntplib`` API.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any

try:  # pragma: no cover - preferred path
    import nntplib as _nntplib  # type: ignore
except Exception:  # pragma: no cover - Python 3.13+
    _nntplib = None  # type: ignore

if _nntplib is not None:  # pragma: no branch - simple
    nntplib = _nntplib
else:  # pragma: no cover - only exercised in tests
    try:
        from aio_nntp import client as _aio_client  # type: ignore
    except Exception:  # pragma: no cover - fallback unavailable
        _aio_client = None  # type: ignore

    if _aio_client is None:  # pragma: no cover - no usable library
        nntplib = None  # type: ignore
    else:

        class _NNTPCompat:
            """Minimal synchronous wrapper around :mod:`aio_nntp`."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._client = _aio_client.NNTP(*args, **kwargs)

            # ``nntplib`` exposes context manager methods; provide equivalents.
            def __enter__(self) -> "_NNTPCompat":  # pragma: no cover - trivial
                return self

            def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
                asyncio.run(self.quit())

            def reader(self) -> Any:
                return asyncio.run(self._client.reader())

            def quit(self) -> Any:
                return asyncio.run(self._client.quit())

            def group(self, name: str) -> Any:
                return asyncio.run(self._client.group(name))

            def xover(self, start: int, end: int) -> Any:
                return asyncio.run(self._client.xover(start, end))

            def body(self, message_id: str, decode: bool = False) -> Any:
                return asyncio.run(self._client.body(message_id, decode=decode))

            def list(self, pattern: str | None = None) -> Any:
                return asyncio.run(self._client.list(pattern))

        nntplib = types.SimpleNamespace(  # type: ignore
            NNTP=_NNTPCompat, NNTP_SSL=_NNTPCompat
        )
