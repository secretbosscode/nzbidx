from __future__ import annotations

import pytest

from nzbidx_ingest.segment_schema import validate_segment_schema


def test_rejects_surrogate_in_message_id() -> None:
    segments = [{"number": 1, "message_id": "m1\ud800", "group": "g", "size": 1}]
    with pytest.raises(AssertionError):
        validate_segment_schema(segments)


def test_rejects_surrogate_in_group() -> None:
    segments = [{"number": 1, "message_id": "m1", "group": "g\udfff", "size": 1}]
    with pytest.raises(AssertionError):
        validate_segment_schema(segments)
