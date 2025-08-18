from nzbidx_ingest.parsers import detect_language


def test_detect_language_token():
    assert detect_language("[FRENCH] film") == "fr"


def test_detect_language_english():
    # Fallback to langdetect should identify English
    lang = detect_language("this is a simple test release")
    assert lang == "en"


def test_detect_language_unknown():
    assert detect_language("") is None


def test_detect_language_noisy_english():
    noisy = "britney spears pregnant - pics and vids @ bustedstars com:2008-09-29"
    assert detect_language(noisy) == "en"


def test_detect_language_disabled(monkeypatch):
    monkeypatch.setenv("DETECT_LANGUAGE", "0")
    import importlib
    import nzbidx_ingest.config as config
    import nzbidx_ingest.parsers as parsers

    importlib.reload(config)
    importlib.reload(parsers)
    assert parsers.detect_language("this is a test") is None

    monkeypatch.delenv("DETECT_LANGUAGE", raising=False)
    importlib.reload(config)
    importlib.reload(parsers)
