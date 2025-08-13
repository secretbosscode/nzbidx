#!/usr/bin/env python3
"""Schedule periodic database maintenance tasks.

This helper uses APScheduler to run VACUUM (ANALYZE), REINDEX and ANALYZE
statements at regular intervals so PostgreSQL statistics and indexes remain
up to date.
"""

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nzbidx_api.db import vacuum_analyze, reindex, analyze


async def main() -> None:
    scheduler = AsyncIOScheduler()
    # Run VACUUM daily at 03:00
    scheduler.add_job(vacuum_analyze, "cron", hour=3)
    # Refresh planner statistics daily at 02:00
    scheduler.add_job(analyze, "cron", hour=2)
    # Rebuild indexes weekly on Sunday at 04:00
    scheduler.add_job(reindex, "cron", day_of_week="sun", hour=4)
    scheduler.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":  # pragma: no cover - manual execution
    asyncio.run(main())
