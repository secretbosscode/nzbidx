from __future__ import annotations

from unittest import mock

from nzbidx_ingest.db_migrations import create_release_posted_at_index


def test_create_release_posted_at_index_retries(monkeypatch):
    conn = mock.Mock()
    cur = mock.Mock()
    conn.cursor.return_value = cur

    # Initial existence check succeeds, partition query fails, fallback succeeds
    cur.execute.side_effect = [None, Exception("boom"), None]
    cur.fetchone.return_value = ("release",)

    create_release_posted_at_index(conn)

    # rollback is called after failure
    conn.rollback.assert_called_once()
    # fallback query executed
    assert cur.execute.call_args_list[2][0][0] == (
        "CREATE INDEX IF NOT EXISTS release_posted_at_idx ON release (posted_at)"
    )
    # commit called at end
    conn.commit.assert_called_once()
    # initial partition query attempted after existence check
    assert "FROM pg_inherits" in cur.execute.call_args_list[1][0][0]
