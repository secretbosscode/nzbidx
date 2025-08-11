import sys
from types import SimpleNamespace
from pathlib import Path

# Ensure the API package is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "services/api/src"))

from nzbidx_api import nzb_builder  # noqa: E402


class DummyNNTP:
    def __init__(
        self, host, port, user=None, password=None, readermode=True, timeout=10
    ):
        pass

    def __enter__(self):  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
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
                    "bytes": "123",
                },
                {
                    "subject": 'MyRelease "testfile.bin" (2/2)',
                    "message-id": "msg2@example.com",
                    "bytes": "456",
                },
            ],
        )


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
