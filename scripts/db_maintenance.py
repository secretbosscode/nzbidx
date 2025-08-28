#!/usr/bin/env python3
"""Schedule periodic database maintenance tasks.

This helper uses APScheduler to run VACUUM (ANALYZE), REINDEX and ANALYZE
statements at regular intervals so PostgreSQL statistics and indexes remain
up to date.
"""

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nzbidx_api.db import analyze, reindex, vacuum_analyze
from prune_disallowed_sizes import prune_sizes
from prune_old_releases import prune_old_releases


async def main() -> None:
    scheduler = AsyncIOScheduler()

    async def prune_disallowed() -> None:
        await asyncio.to_thread(prune_sizes)

    async def prune_old() -> None:
        await asyncio.to_thread(prune_old_releases)

    # Run VACUUM daily at 03:00
    scheduler.add_job(vacuum_analyze, "cron", hour=3)
    # Refresh planner statistics daily at 02:00
    scheduler.add_job(analyze, "cron", hour=2)
    # Rebuild indexes weekly on Sunday at 04:00
    scheduler.add_job(reindex, "cron", day_of_week="sun", hour=4)
    # Prune releases outside configured size thresholds daily at 01:00
    scheduler.add_job(prune_disallowed, "cron", hour=1)
    # Remove releases older than the retention window daily at 00:00
    scheduler.add_job(prune_old, "cron", hour=0)
    scheduler.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":  # pragma: no cover - manual execution
    asyncio.run(main())
