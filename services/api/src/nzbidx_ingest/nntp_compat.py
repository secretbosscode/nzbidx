"""Compatibility layer for NNTP clients.

Attempt to import :mod:`nntplib`.  On Python versions where the module has
been removed from the standard library (3.13+), the third-party
``standard-nntplib`` package provides an identical replacement.  If neither
is available ``nntplib`` is set to ``None`` so callers can fail gracefully.
"""

from __future__ import annotations

try:  # pragma: no cover - simple import
    import nntplib  # type: ignore
except Exception:  # pragma: no cover - missing library
    nntplib = None  # type: ignore
