"""CLI entry point for the ingest loop."""

from .ingest_loop import run_forever
from .resource_monitor import install_signal_handlers, start_memory_logger

if __name__ == "__main__":  # pragma: no cover - script entry
    install_signal_handlers()
    start_memory_logger()
    run_forever()
