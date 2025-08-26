"""Helpers for validating release segment schema."""

from __future__ import annotations

from typing import Any, Iterable

EXPECTED_SEGMENT_KEYS = {"number", "message_id", "group", "size"}


def validate_segment_schema(segments: Iterable[dict[str, Any]]) -> None:
    """Raise ``AssertionError`` if ``segments`` do not match the dict schema."""
    for seg in segments:
        if not isinstance(seg, dict):
            raise AssertionError(f"segment entry is not a dict: {seg!r}")
        if set(seg.keys()) != EXPECTED_SEGMENT_KEYS:
            raise AssertionError(f"segment keys mismatch: {seg!r}")
        if not isinstance(seg.get("number"), int):
            raise AssertionError(f"segment number must be int: {seg!r}")
        msg_id = seg.get("message_id")
        if not isinstance(msg_id, str):
            raise AssertionError(f"segment message_id must be str: {seg!r}")
        if "<" in msg_id or ">" in msg_id:
            raise AssertionError(
                f"segment message_id must not contain angle brackets: {seg!r}"
            )
        if not isinstance(seg.get("group"), str):
            raise AssertionError(f"segment group must be str: {seg!r}")
        if not isinstance(seg.get("size"), int):
            raise AssertionError(f"segment size must be int: {seg!r}")
