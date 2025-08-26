from pathlib import Path

import threading

import nzbidx_ingest.resource_monitor as rm


def test_get_memory_stats_cgroup_v2(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(rm, "_USED_PATH", tmp_path / "memory.current")
    monkeypatch.setattr(rm, "_LIMIT_PATH", tmp_path / "memory.max")
    (tmp_path / "memory.current").write_text("100")
    (tmp_path / "memory.max").write_text("200")
    used, limit = rm.get_memory_stats()
    assert used == 100
    assert limit == 200


def test_get_memory_stats_unlimited(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(rm, "_USED_PATH", tmp_path / "memory.current")
    monkeypatch.setattr(rm, "_LIMIT_PATH", tmp_path / "memory.max")
    (tmp_path / "memory.current").write_text("100")
    # value larger than 2**60 should be treated as unlimited
    (tmp_path / "memory.max").write_text(str(1 << 63))
    used, limit = rm.get_memory_stats()
    assert used == 100
    assert limit is None


def test_get_memory_stats_cgroup_v1(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(rm, "_USED_PATH", tmp_path / "memory.usage_in_bytes")
    monkeypatch.setattr(rm, "_LIMIT_PATH", tmp_path / "memory.limit_in_bytes")
    (tmp_path / "memory.usage_in_bytes").write_text("150")
    (tmp_path / "memory.limit_in_bytes").write_text("300")
    used, limit = rm.get_memory_stats()
    assert used == 150
    assert limit == 300


def test_start_memory_logger_returns_event(tmp_path: Path) -> None:
    stop = rm.start_memory_logger(interval=0, root=tmp_path)
    assert isinstance(stop, threading.Event)
    stop.set()
