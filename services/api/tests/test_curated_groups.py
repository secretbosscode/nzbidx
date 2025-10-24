import importlib
import sqlite3

from nzbidx_ingest import config, cursors, main


def test_curated_mode_prunes_releases(monkeypatch):
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setenv("NNTP_GROUP_MODE", "curated")
    monkeypatch.setenv("NNTP_CURATED_GROUPS", "alt.keep.one,alt.keep.two")

    importlib.reload(config)
    importlib.reload(main)

    groups = config.get_nntp_groups()
    assert groups == ["alt.keep.one", "alt.keep.two"]

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE release (norm_title TEXT, source_group TEXT)")
    conn.executemany(
        "INSERT INTO release (norm_title, source_group) VALUES (?, ?)",
        [
            ("keep-1", "alt.keep.one"),
            ("drop-1", "alt.drop.me"),
            ("drop-null", None),
            ("drop-empty", ""),
            ("keep-2", "alt.keep.two"),
        ],
    )

    main.prune_non_curated_groups(conn, groups)

    rows = conn.execute(
        "SELECT norm_title, source_group FROM release ORDER BY norm_title"
    ).fetchall()
    assert rows == [
        ("keep-1", "alt.keep.one"),
        ("keep-2", "alt.keep.two"),
    ]

    # Reset cached state for other tests.
    config.set_nntp_groups(None)


def test_curated_mode_refreshes_cached_groups(monkeypatch):
    monkeypatch.setenv("NNTP_GROUP_MODE", "configured")
    monkeypatch.setenv("NNTP_GROUPS", "alt.initial")

    importlib.reload(config)

    groups = config.get_nntp_groups()
    assert groups == ["alt.initial"]

    monkeypatch.setenv("NNTP_GROUP_MODE", "curated")
    monkeypatch.setenv("NNTP_CURATED_GROUPS", "alt.curated.one,alt.curated.two")
    monkeypatch.delenv("NNTP_GROUPS", raising=False)

    groups = config.get_nntp_groups()
    assert groups == ["alt.curated.one", "alt.curated.two"]

    # Reset cached state for other tests.
    config.set_nntp_groups(None)
    monkeypatch.delenv("NNTP_CURATED_GROUPS", raising=False)
    monkeypatch.delenv("NNTP_GROUP_MODE", raising=False)
    importlib.reload(config)


def test_curated_mode_resets_cursor_state(monkeypatch, tmp_path):
    monkeypatch.setenv("CURSOR_DB", str(tmp_path / "cursors.sqlite"))

    importlib.reload(config)
    importlib.reload(cursors)

    cursors.set_cursor("alt.old", 42)
    cursors.mark_irrelevant("alt.skip")
    cursors.mark_group_mode("auto")

    reset = cursors.reset_for_curated()
    assert reset is True
    assert cursors.get_cursors(["alt.old"]) == {}
    assert cursors.get_irrelevant_groups() == []

    # Subsequent curated resets become no-ops until the mode changes again.
    assert cursors.reset_for_curated() is False

    cursors.mark_group_mode("auto")
    assert cursors.reset_for_curated() is True

    # Reset cached state for other tests.
    monkeypatch.delenv("CURSOR_DB", raising=False)
    importlib.reload(config)
    importlib.reload(cursors)
