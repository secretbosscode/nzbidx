import nzbidx_ingest.ingest_loop as loop  # type: ignore
from nzbidx_ingest import config, cursors  # type: ignore


def test_group_recovers_after_outage(monkeypatch):
    loop._group_failures.clear()
    loop._group_probes.clear()

    # Control monotonic time
    now = [0.0]

    def monotonic() -> float:
        return now[0]

    monkeypatch.setattr(loop.time, "monotonic", monotonic)
    monkeypatch.setattr(loop.time, "sleep", lambda _s: None)

    # Basic configuration
    monkeypatch.setattr(config, "NNTP_GROUPS", ["alt.test"], raising=False)
    monkeypatch.setattr(cursors, "get_cursor", lambda _g: 0)
    monkeypatch.setattr(cursors, "set_cursor", lambda _g, _c: None)
    monkeypatch.setattr(cursors, "mark_irrelevant", lambda _g: None)
    monkeypatch.setattr(cursors, "get_irrelevant_groups", lambda: set())

    # Stub out heavy helpers
    monkeypatch.setattr(loop, "connect_db", lambda: None)
    monkeypatch.setattr(
        loop, "insert_release", lambda db, releases: {r[0] for r in releases}
    )
    monkeypatch.setattr(loop, "min_size_for_release", lambda _t, _c: 0)
    monkeypatch.setattr(loop, "validate_segment_schema", lambda _s: None)
    monkeypatch.setattr(loop, "normalize_subject", lambda s, with_tags=False: (s, []))
    monkeypatch.setattr(loop, "detect_language", lambda _s: "en")
    monkeypatch.setattr(loop, "extract_segment_number", lambda _s: 1)
    monkeypatch.setattr(loop, "extract_file_extension", lambda _s: "mkv")
    monkeypatch.setattr(loop, "_infer_category", lambda _s, _g: "0")

    class DummyClient:
        def __init__(self):
            self.fail = True
            self.high_calls = 0
            self.xover_calls = 0

        def connect(self) -> None:
            pass

        def high_water_mark(self, _group: str) -> int:
            self.high_calls += 1
            return 0 if self.fail else 1

        def xover(self, _group: str, _start: int, _end: int):
            self.xover_calls += 1
            return [{":bytes": "100", "subject": "Example (1/1)", "message-id": "<m1>"}]

        def body_size(self, _mid: str) -> int:
            return 100

    client = DummyClient()

    # First run - schedule probe
    loop.run_once(client)
    assert "alt.test" in loop._group_probes
    scheduled = loop._group_probes["alt.test"]
    first_calls = client.high_calls

    # Second run before probe time - skipped
    loop.run_once(client)
    assert client.high_calls == first_calls
    assert loop._group_probes["alt.test"] == scheduled

    # Advance time and restore connectivity
    now[0] = scheduled
    client.fail = False
    loop.run_once(client)
    assert "alt.test" not in loop._group_probes
    assert client.xover_calls > 0
    assert loop._group_failures.get("alt.test", 0) == 0
