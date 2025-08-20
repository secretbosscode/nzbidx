from __future__ import annotations

from nzbidx_api import search as search_mod  # type: ignore


def test_search_releases_invalid_sort(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_text(query: str) -> str:
        captured["query"] = query
        return query

    monkeypatch.setattr(search_mod, "get_engine", lambda: None)
    monkeypatch.setattr(search_mod, "text", fake_text)

    search_mod.search_releases(None, limit=1, sort="invalid")

    assert "ORDER BY posted_at DESC" in captured["query"]
