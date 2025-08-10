"""Log sanitizer removes API keys from log records."""

import logging
from pathlib import Path
import sys

# Ensure importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api.log_sanitize import LogSanitizerFilter  # noqa: E402


def test_logs_scrub_api_key(caplog):
    logger = logging.getLogger("scrub-test")
    logger.addFilter(LogSanitizerFilter())
    with caplog.at_level("INFO"):
        logger.info("msg", extra={"headers": {"X-Api-Key": "secret", "Other": "ok"}})
    record = caplog.records[0]
    assert record.headers["X-Api-Key"] == "[redacted]"
    assert record.headers["Other"] == "ok"
