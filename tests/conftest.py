import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

API_SRC = ROOT / "services" / "api" / "src"
if str(API_SRC) not in sys.path:
    sys.path.insert(0, str(API_SRC))

import pytest

from nzbidx_api import config as api_config


@pytest.fixture(autouse=True)
def _clear_validate_cache() -> None:
    """Ensure NNTP config validation cache is reset between tests."""

    api_config.clear_validate_cache()
