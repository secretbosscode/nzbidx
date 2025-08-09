"""Parsers for ingest service."""

from __future__ import annotations

import re


def parse() -> None:
    """Parse data stub."""
    pass


def normalize_subject(subject: str) -> str:
    """Return a normalized version of an NNTP subject line.

    The normalisation is intentionally simple for now – it lowers the case and
    collapses repeating whitespace so tests can reason about duplicate
    detection.  Real-world logic can be much more sophisticated.
    """

    # Replace any amount of whitespace with a single space and lowercase the
    # result.  Strip leading/trailing whitespace so titles can be compared.
    return re.sub(r"\s+", " ", subject).strip().lower()
