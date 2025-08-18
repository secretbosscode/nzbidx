from __future__ import annotations

import logging
import sys


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
        self.records.append(record)


def test_setup_logging_single_execution() -> None:
    """Repeated setup calls should not duplicate log entries."""
    root = logging.getLogger()
    access = logging.getLogger("uvicorn.access")
    root_handlers, root_level, root_filters = (
        list(root.handlers),
        root.level,
        list(root.filters),
    )
    access_handlers, access_prop = list(access.handlers), access.propagate

    try:
        for mod in ["nzbidx_api.main", "nzbidx_ingest.logging"]:
            if mod in sys.modules:
                del sys.modules[mod]

        import nzbidx_api.main as api_main  # type: ignore
        import nzbidx_ingest.logging as ingest_logging  # type: ignore

        ingest_logging.setup_logging()
        api_main.setup_logging()

        collector = ListHandler()
        root.addHandler(collector)
        access.addHandler(collector)

        logging.getLogger().info("root")
        logging.getLogger("uvicorn.access").info("access")

        messages = [r.getMessage() for r in collector.records]
        assert messages.count("root") == 1
        assert messages.count("access") == 1
    finally:
        root.handlers = root_handlers
        root.setLevel(root_level)
        root.filters = root_filters
        access.handlers = access_handlers
        access.propagate = access_prop
        if hasattr(root, "_nzbidx_logging_configured"):
            delattr(root, "_nzbidx_logging_configured")
        for mod in ["nzbidx_api.main", "nzbidx_ingest.logging"]:
            if mod in sys.modules:
                del sys.modules[mod]
