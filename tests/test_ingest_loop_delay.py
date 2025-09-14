import logging

import nzbidx_ingest.ingest_loop as loop
import pytest


class DummyEvent:
    def __init__(self):
        self.delay = None

    def is_set(self) -> bool:
        return False

    def wait(self, delay: float) -> bool:
        self.delay = delay
        return True


@pytest.mark.parametrize("invalid", [None, float("nan"), -1.0])
def test_run_forever_invalid_delay(monkeypatch, invalid, caplog):
    def fake_run_once():
        return invalid

    monkeypatch.setattr(loop, "run_once", fake_run_once)
    event = DummyEvent()
    with caplog.at_level(logging.ERROR):
        loop.run_forever(stop_event=event)
    assert event.delay == loop.INGEST_POLL_MIN_SECONDS
    assert any(record.message == "ingest_delay_invalid" for record in caplog.records)
