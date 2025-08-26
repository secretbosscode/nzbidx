from __future__ import annotations

from nzbidx_ingest.parsers import normalize_subject  # type: ignore


def test_normalize_subject_strips_misc_noise() -> None:
    subject = " -[FRENCH] Example.part01.rar yEnc (01/15) repost sample- "
    assert normalize_subject(subject) == "example"
    assert normalize_subject("Example [123/456]") == "example"
