"""Data models for the API service."""

from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass
class Release:
    """Representation of a usenet release."""

    id: int
    norm_title: str
    category: Optional[str] = None
    language: Optional[str] = None
    tags: Sequence[str] = ()
