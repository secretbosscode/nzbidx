"""CLI entry point for the ingest loop."""

from .ingest_loop import run_forever

if __name__ == "__main__":  # pragma: no cover - script entry
    run_forever()
