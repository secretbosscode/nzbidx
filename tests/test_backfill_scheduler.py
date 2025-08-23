from __future__ import annotations

import asyncio
import importlib


def test_scheduler_triggers(monkeypatch):
    monkeypatch.setenv("BACKFILL_INTERVAL_SECONDS", "1")
    import nzbidx_api.main as main

    importlib.reload(main)

    async def runner():
        event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def dummy(*args, **kwargs):
            loop.call_soon_threadsafe(event.set)
            return 0

        monkeypatch.setattr(main, "backfill_release_parts", dummy)
        await main.start_backfill_scheduler()
        try:
            await asyncio.wait_for(event.wait(), timeout=1.5)
        finally:
            await main.stop_backfill_scheduler()

    asyncio.run(runner())
