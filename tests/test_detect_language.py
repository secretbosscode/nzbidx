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


def test_detect_language_enabled(monkeypatch):
    monkeypatch.setenv("DETECT_LANGUAGE", "true")
    import importlib
    import nzbidx_ingest.config as config
    import nzbidx_ingest.parsers as parsers

    importlib.reload(config)
    importlib.reload(parsers)
    assert parsers.detect_language("this is a test") == "en"

    monkeypatch.delenv("DETECT_LANGUAGE", raising=False)
    importlib.reload(config)
    importlib.reload(parsers)


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


def test_detect_language_python_guard(monkeypatch):
    import collections
    import importlib
    import sys
    import types

    VersionInfo = collections.namedtuple(
        "VersionInfo", "major minor micro releaselevel serial"
    )

    original_version = sys.version_info
    guard_version = VersionInfo(3, 13, 0, "final", 0)
    monkeypatch.setattr(sys, "version_info", guard_version)

    import nzbidx_ingest.config as config
    import nzbidx_ingest.parsers as parsers

    class FailLangdetect(types.ModuleType):
        def __getattr__(self, name: str) -> object:  # pragma: no cover - defensive
            raise AssertionError("langdetect should not be imported when guarded")

    original_langdetect = sys.modules.get("langdetect")
    sys.modules["langdetect"] = FailLangdetect("langdetect")

    importlib.reload(config)
    importlib.reload(parsers)

    try:
        assert parsers._PYTHON_UNSUPPORTED_FOR_LANGDETECT is True
        assert parsers.detect is None
        assert parsers.detect_language("ascii only subject") == "en"
        assert parsers.detect_language("你好") is None
    finally:
        if original_langdetect is None:
            sys.modules.pop("langdetect", None)
        else:
            sys.modules["langdetect"] = original_langdetect

        monkeypatch.setattr(sys, "version_info", original_version)
        importlib.reload(config)
        importlib.reload(parsers)


def test_detect_language_python_guard_clears_stale_detect(monkeypatch):
    import collections
    import importlib
    import sys
    import types

    subject = "ascii only subject"

    import nzbidx_ingest.config as config
    import nzbidx_ingest.parsers as parsers

    sentinel_calls: list[str] = []

    def stale_detect(text: str) -> str:
        sentinel_calls.append(text)
        return "xx"

    parsers.detect = stale_detect  # type: ignore[attr-defined]
    parsers._detect_language_cached.cache_clear()
    assert parsers.detect_language(subject) == "xx"
    assert sentinel_calls == [subject]

    VersionInfo = collections.namedtuple(
        "VersionInfo", "major minor micro releaselevel serial"
    )
    original_version = sys.version_info
    guard_version = VersionInfo(3, 13, 0, "final", 0)
    monkeypatch.setattr(sys, "version_info", guard_version)

    fail_calls: list[str] = []

    class FailLangdetect(types.ModuleType):
        def __getattr__(self, name: str) -> object:  # pragma: no cover - defensive
            raise AssertionError(f"unexpected attribute access: {name}")

    fail_module = FailLangdetect("langdetect")

    def failing_detect(text: str) -> str:
        fail_calls.append(text)
        return "zz"

    fail_module.detect = failing_detect  # type: ignore[attr-defined]

    original_langdetect = sys.modules.get("langdetect")
    sys.modules["langdetect"] = fail_module

    importlib.reload(config)
    importlib.reload(parsers)

    try:
        assert parsers._PYTHON_UNSUPPORTED_FOR_LANGDETECT is True
        assert parsers.detect is None
        assert parsers.detect_language(subject) == "en"
        assert fail_calls == []
        assert sentinel_calls == [subject]
    finally:
        if original_langdetect is None:
            sys.modules.pop("langdetect", None)
        else:
            sys.modules["langdetect"] = original_langdetect

        monkeypatch.setattr(sys, "version_info", original_version)
        importlib.reload(config)
        importlib.reload(parsers)
