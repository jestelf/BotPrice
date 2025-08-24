import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .config import settings, presets
from .queue import AbstractQueue

class Orchestrator:
    def __init__(self, queue: AbstractQueue):
        self.queue = queue
        self.scheduler = AsyncIOScheduler()
        self.running = False

    async def start(self):
        self.running = True

        # План: каждый час тихий сбор + подборки в 09:00 и 19:00
        self.scheduler.add_job(self.run_all_presets_and_notify, CronTrigger.from_crontab("0 9,19 * * *"))
        self.scheduler.add_job(self.run_all_presets_no_notify, "interval", minutes=60)
        self.scheduler.start()

    async def stop(self):
        self.scheduler.shutdown(wait=False)
        self.running = False

    async def run_all_presets_no_notify(self):
        await self._run_presets(notify=False)

    async def run_all_presets_and_notify(self):
        await self._run_presets(notify=True)

    async def _run_presets(self, notify: bool):
        geoid = presets.geoid_default or settings.DEFAULT_GEOID
        min_discount = settings.MIN_DISCOUNT
        min_score = settings.MIN_SCORE

        # ozon
        for item in presets.sites.get("ozon", []):
            await self.queue.publish({
                "site": "ozon",
                "url": item["url"],
                "geoid": "",
                "min_discount": min_discount,
                "min_score": min_score,
                "notify": notify,
            })
            await asyncio.sleep(1.0)

        # market
        for item in presets.sites.get("market", []):
            await self.queue.publish({
                "site": "market",
                "url": item["url"],
                "geoid": geoid,
                "min_discount": min_discount,
                "min_score": min_score,
                "notify": notify,
            })
            await asyncio.sleep(1.0)
