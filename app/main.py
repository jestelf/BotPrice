import asyncio
from .db import init_db
from .orchestrator import Orchestrator
from .config import settings
from .queue import RedisQueue

import sentry_sdk

async def main():
    if settings.SENTRY_DSN:
        sentry_sdk.init(settings.SENTRY_DSN)
    await init_db()
    queue = RedisQueue(settings.REDIS_URL, settings.QUEUE_STREAM)
    orch = Orchestrator(queue)
    await orch.start()

    print("Orchestrator started. Hourly scraping + 09:00/19:00 digests enabled.")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        print("Stopping...")
    finally:
        await orch.stop()

if __name__ == "__main__":
    asyncio.run(main())
