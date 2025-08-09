"""Entry point for the ingest service."""

from __future__ import annotations

from dotenv import load_dotenv

from .logging import setup_logging
from .nntp_client import NNTPClient


def main() -> int:
    """Run the ingest service."""
    load_dotenv()
    setup_logging()
    client = NNTPClient()
    client.connect()
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
