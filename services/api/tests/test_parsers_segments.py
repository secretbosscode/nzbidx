import pytest

from nzbidx_ingest.parsers import extract_segment_number


@pytest.mark.parametrize(
    "subject, expected",
    [
        ("some file (1/10)", 1),
        ("prefix (12/34) suffix", 12),
    ],
)
def test_extract_segment_number_valid(subject: str, expected: int) -> None:
    assert extract_segment_number(subject) == expected


@pytest.mark.parametrize(
    "subject",
    [
        "no segments",
        "(abc/123)",
        "(1/)",
        "(1/2",
    ],
)
def test_extract_segment_number_malformed(subject: str) -> None:
    assert extract_segment_number(subject) == 1
