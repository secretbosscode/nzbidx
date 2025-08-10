from pathlib import Path
import sys
import asyncio

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
import nzbidx_api.main as main  # noqa: E402


class Dummy:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_shutdown_closes_clients() -> None:
    main.opensearch = Dummy()
    main.cache = Dummy()
    asyncio.run(main.shutdown())
    assert getattr(main.opensearch, "closed", True)
    assert getattr(main.cache, "closed", True)
