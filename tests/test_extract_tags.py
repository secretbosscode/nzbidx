from nzbidx_ingest.parsers import extract_tags
from nzbidx_ingest import parsers


def test_extract_tags_without_brackets(monkeypatch):
    class FailPattern:
        def finditer(self, _):
            raise AssertionError("regex should not run")

    monkeypatch.setattr(parsers, "_TAG_RE", FailPattern())
    assert extract_tags("This subject has no tags") == []


def test_extract_tags_with_brackets():
    assert extract_tags("[Foo Bar] [Baz]") == ["foo", "bar", "baz"]
