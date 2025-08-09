"""Tests for booting the ingest service."""

from __future__ import annotations

from nzbidx_ingest.main import main


def test_boot_dry_run(monkeypatch, capsys) -> None:
    """Running with no NNTP configuration should log a dry-run message."""
    monkeypatch.delenv("NNTP_HOST_1", raising=False)
    exit_code = main()
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "dry-run: no NNTP providers configured" in captured.out
