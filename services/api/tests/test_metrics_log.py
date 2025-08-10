from pathlib import Path
import sys
import logging

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from nzbidx_api import metrics_log  # noqa: E402


class FakeEvent:
    def __init__(self) -> None:
        self.calls = 0

    def wait(self, _interval: int) -> bool:
        self.calls += 1
        return self.calls > 1

    def set(self) -> None:  # pragma: no cover - no behaviour needed
        self.calls = 2


class FakeThread:
    def __init__(self, target: callable, daemon: bool) -> None:  # noqa: D401
        self._target = target

    def start(self) -> None:
        self._target()


def test_metrics_emit_interval(monkeypatch, caplog):
    metrics_log._counters.clear()
    metrics_log._gauges.clear()
    metrics_log._prev_counters.clear()
    metrics_log._prev_gauges.clear()
    monkeypatch.setattr(metrics_log.threading, "Event", FakeEvent)
    monkeypatch.setattr(metrics_log.threading, "Thread", FakeThread)
    with caplog.at_level(logging.INFO):
        metrics_log.inc_rate_limited()
        metrics_log.set_ingest_lag(3)
        stop = metrics_log.start(interval=1)
    metrics = {rec.metric: rec.value for rec in caplog.records}
    assert metrics["rate_limited_total"] == 1
    assert metrics["ingest_lag_articles"] == 3
    stop()
