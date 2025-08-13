import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "services" / "api" / "src"))

from nzbidx_ingest.parsers import detect_language

def test_detect_language_token():
    assert detect_language('[FRENCH] film') == 'fr'


def test_detect_language_english():
    # Fallback to langdetect should identify English
    lang = detect_language('this is a simple test release')
    assert lang == 'en'


def test_detect_language_unknown():
    assert detect_language('') is None
