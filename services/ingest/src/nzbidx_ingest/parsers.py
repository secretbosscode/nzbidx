"""Parsers for ingest service."""

from __future__ import annotations

import re


def normalize_subject(subject: str) -> str:
    """Return a cleaned, human-readable version of a Usenet subject line.

    Lightweight normalization:
    - Convert separators ('.', '_') to spaces
    - Remove explicit 'yEnc' markers
    - Drop part counters like '(01/15)' or '[12345/12346]'
    - Remove common filler words (e.g., 'repost', 'sample')
    - Collapse whitespace and trim separators
    """
    if not subject:
        return ""

    # Convert common separators to spaces.
    cleaned = subject.replace(".", " ").replace("_", " ")

    # Remove explicit yEnc markers.
    cleaned = re.sub(r"(?i)\byenc\b", "", cleaned)

    # Drop part/size information such as "(01/15)" or "[12345/12346]".
    cleaned = re.sub(r"[\(\[]\s*\d+\s*/\s*\d+\s*[\)\]]", "", cleaned)

    # Remove common filler words.
    fillers = ("repost", "sample")
    cleaned = re.sub(
        rf"\b({'|'.join(map(re.escape, fillers))})\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    # Collapse whitespace and trim leading/trailing separators or dashes.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"^[-\s]+|[-\s]+$", "", cleaned)

    # Return the normalized subject preserving original case.
    return cleaned


def parse() -> None:
    """Parse data stub."""
    pass
