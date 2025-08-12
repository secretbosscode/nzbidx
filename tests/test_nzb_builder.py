import sys
from types import SimpleNamespace
from pathlib import Path

# Ensure the API package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "services/api/src"))

from nzbidx_api import nzb_builder  # noqa: E402


class DummyNNTP:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):  # pragma: no cover - trivial
        return self

    def __exit__(self, _exc_type, _exc, _tb):  # pragma: no cover - trivial
        pass

    def group(self, group):
        # resp, count, first, last, name
        return ("", 2, "1", "2", group)

    def xover(self, start, end):
        return (
            "",
            [
                {
                    "subject": 'MyRelease "testfile.bin" (1/2)',
                    "message-id": "msg1@example.com",
                },
                {
                    "subject": 'MyRelease "testfile.bin" (2/2)',
                    "message-id": "msg2@example.com",
                },
            ],
        )

    def body(self, message_id, decode=False):  # pragma: no cover - simple
        if message_id == "msg1@example.com":
            lines = [b"a" * 123]
        else:
            lines = [b"b" * 456]
        return "", 0, message_id, lines


def test_builds_nzb(monkeypatch):
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.setenv("NNTP_GROUPS", "alt.binaries.example")
    monkeypatch.setattr(
        nzb_builder, "nntplib", SimpleNamespace(NNTP=DummyNNTP, NNTP_SSL=DummyNNTP)
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "msg1@example.com" in xml
    assert "msg2@example.com" in xml
    assert '<segment bytes="123" number="1">msg1@example.com</segment>' in xml
    assert '<segment bytes="456" number="2">msg2@example.com</segment>' in xml


class AutoNNTP(DummyNNTP):
    def list(self):
        return "", [("alt.binaries.example", "0", "0", "0")]


def test_builds_nzb_auto_groups(monkeypatch):
    monkeypatch.setenv("NNTP_HOST", "example.com")
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setattr(
        nzb_builder, "nntplib", SimpleNamespace(NNTP=AutoNNTP, NNTP_SSL=AutoNNTP)
    )
    xml = nzb_builder.build_nzb_for_release("MyRelease")
    assert "msg1@example.com" in xml
    assert "msg2@example.com" in xml
