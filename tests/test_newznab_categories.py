"""Tests for loading category configuration."""

import logging

import pytest

from nzbidx_api import newznab


@pytest.mark.parametrize("setup", ["missing", "badjson"])
def test_load_categories_warns_on_bad_config(
    monkeypatch, caplog, tmp_path, setup
) -> None:
    """Invalid config path or JSON should emit a warning and use defaults."""

    if setup == "missing":
        cfg_path = tmp_path / "missing.json"
        expected = "category config file not found"
    else:
        cfg_path = tmp_path / "bad.json"
        cfg_path.write_text("{invalid", encoding="utf-8")
        expected = "invalid JSON"

    monkeypatch.setenv("CATEGORY_CONFIG", str(cfg_path))
    with caplog.at_level(logging.WARNING):
        categories = newznab._load_categories()

    assert expected in caplog.text
    assert categories == newznab._default_categories()


def test_caps_xml_includes_adult_categories() -> None:
    """caps.xml should list XXX categories."""

    xml = newznab.caps_xml()

    assert '<category id="6000"' in xml
    assert '<category id="6090"' in xml
