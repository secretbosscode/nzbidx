#!/usr/bin/env python3
# ruff: noqa: E402
"""Schedule periodic database maintenance tasks.

This helper uses APScheduler to run VACUUM (ANALYZE), REINDEX and ANALYZE
statements at regular intervals so PostgreSQL statistics and indexes remain
up to date.
"""

import asyncio
import sys
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Ensure local packages are importable when running from the repo root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "services" / "api" / "src"))

from nzbidx_api.db import analyze, reindex, vacuum_analyze
from prune_disallowed_sizes import prune_sizes
from nzbidx_ingest.main import prune_disallowed_filetypes


async def main() -> None:
    scheduler = AsyncIOScheduler()

    async def prune_disallowed() -> None:
        removed_sizes = await asyncio.to_thread(prune_sizes)
        removed_types = await asyncio.to_thread(prune_disallowed_filetypes)
        print(f"pruned {removed_sizes} releases by size")
        print(f"pruned {removed_types} releases by filetype")

    # Run VACUUM daily at 03:00
    scheduler.add_job(vacuum_analyze, "cron", hour=3)
    # Refresh planner statistics daily at 02:00
    scheduler.add_job(analyze, "cron", hour=2)
    # Rebuild indexes weekly on Sunday at 04:00
    scheduler.add_job(reindex, "cron", day_of_week="sun", hour=4)
    # Prune releases outside configured size thresholds daily at 01:00
    scheduler.add_job(prune_disallowed, "cron", hour=1)
    scheduler.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":  # pragma: no cover - manual execution
    asyncio.run(main())
