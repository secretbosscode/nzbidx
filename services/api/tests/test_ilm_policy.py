from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
import nzbidx_api.main as main  # noqa: E402


def test_ilm_policy_env(monkeypatch) -> None:
    monkeypatch.setenv("ILM_DELETE_DAYS", "1")
    monkeypatch.setenv("ILM_WARM_DAYS", "2")
    policy = main.build_ilm_policy()
    warm = policy["policy"]["phases"]["warm"]["min_age"]
    delete = policy["policy"]["phases"]["delete"]["min_age"]
    assert warm == "2d"
    assert delete == "1d"
