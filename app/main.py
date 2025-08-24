import asyncio
from .db import init_db
from .orchestrator import Orchestrator
from .config import settings

async def main():
    await init_db()
    orch = Orchestrator()
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
