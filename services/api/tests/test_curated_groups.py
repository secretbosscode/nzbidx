import importlib
import sqlite3

from nzbidx_ingest import config, main


def test_curated_mode_prunes_releases(monkeypatch):
    monkeypatch.delenv("NNTP_GROUPS", raising=False)
    monkeypatch.setenv("NNTP_GROUP_MODE", "curated")
    monkeypatch.setenv("NNTP_CURATED_GROUPS", "alt.keep.one,alt.keep.two")

    importlib.reload(config)
    importlib.reload(main)

    groups = config.get_nntp_groups()
    assert groups == ["alt.keep.one", "alt.keep.two"]

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE release (norm_title TEXT, source_group TEXT)"
    )
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
