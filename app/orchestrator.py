import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.ext.asyncio import AsyncSession
from .db import SessionLocal
from .config import settings, presets
from .scraper.render import RenderService
from .processing.pipeline import process_preset
from .notifier.bot import send_batch

class Orchestrator:
    def __init__(self):
        self.render = RenderService()
        self.scheduler = AsyncIOScheduler()
        self.running = False

    async def start(self):
        await self.render.start()
        self.running = True

        # План: каждый час тихий сбор + подборки в 09:00 и 19:00
        self.scheduler.add_job(self.run_all_presets_and_notify, CronTrigger.from_crontab("0 9,19 * * *"))
        self.scheduler.add_job(self.run_all_presets_no_notify, "interval", minutes=60)
        self.scheduler.start()

    async def stop(self):
        self.scheduler.shutdown(wait=False)
        await self.render.stop()
        self.running = False

    async def run_all_presets_no_notify(self):
        async with SessionLocal() as session:
            await self._run_presets(session, notify=False)

    async def run_all_presets_and_notify(self):
        async with SessionLocal() as session:
            await self._run_presets(session, notify=True)

    async def _run_presets(self, session: AsyncSession, notify: bool):
        geoid = presets.geoid_default or settings.DEFAULT_GEOID
        min_discount = settings.MIN_DISCOUNT
        min_score = settings.MIN_SCORE

        all_results: list[dict] = []

        # ozon
        for item in presets.sites.get("ozon", []):
            res = await process_preset(session, self.render, "ozon", item["url"], None, min_discount, min_score)
            all_results.extend(res)
            await asyncio.sleep(1.0)

        # market
        for item in presets.sites.get("market", []):
            res = await process_preset(session, self.render, "market", item["url"], geoid, min_discount, min_score)
            all_results.extend(res)
            await asyncio.sleep(1.0)

        # Отправка батчем — если включён фиксированный чат
        if notify and settings.TG_CHAT_ID and all_results:
            # ограничим размер подборки 20 лучших
            best = sorted(all_results, key=lambda x: x["score"], reverse=True)[:20]
            await send_batch(settings.TELEGRAM_BOT_TOKEN, settings.TG_CHAT_ID, best)
