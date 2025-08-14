from pathlib import Path

from nzbidx_ingest.resource_monitor import get_memory_stats


def test_get_memory_stats_cgroup_v2(tmp_path: Path) -> None:
    (tmp_path / "memory.current").write_text("100")
    (tmp_path / "memory.max").write_text("200")
    used, limit = get_memory_stats(tmp_path)
    assert used == 100
    assert limit == 200


def test_get_memory_stats_unlimited(tmp_path: Path) -> None:
    (tmp_path / "memory.current").write_text("100")
    # value larger than 2**60 should be treated as unlimited
    (tmp_path / "memory.max").write_text(str(1 << 63))
    used, limit = get_memory_stats(tmp_path)
    assert used == 100
    assert limit is None


def test_get_memory_stats_cgroup_v1(tmp_path: Path) -> None:
    (tmp_path / "memory.usage_in_bytes").write_text("150")
    (tmp_path / "memory.limit_in_bytes").write_text("300")
    used, limit = get_memory_stats(tmp_path)
    assert used == 150
    assert limit == 300
