import asyncio
import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from .config import settings, presets
from .queue import AbstractQueue
from .db import SessionLocal
from .models import User
from . import metrics

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(
        self,
        queue: AbstractQueue,
        max_pages: int | None = None,
        max_tasks: int | None = None,
        quiet_hours: tuple[int, int] | str | None = None,
    ):
        self.queue = queue
        self.scheduler = AsyncIOScheduler()
        self.running = False
        self.max_pages = max_pages if max_pages is not None else settings.BUDGET_MAX_PAGES
        self.max_tasks = max_tasks if max_tasks is not None else settings.BUDGET_MAX_TASKS
        qh = quiet_hours if quiet_hours is not None else settings.QUIET_HOURS
        if isinstance(qh, str) and "-" in qh:
            try:
                start, end = map(int, qh.split("-"))
                self.quiet_hours = (start, end)
            except Exception:
                self.quiet_hours = None
        else:
            self.quiet_hours = qh
        self.pages_sent = 0
        self.tasks_sent = 0

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

    def _in_quiet_hours(self) -> bool:
        if not self.quiet_hours:
            return False
        start, end = self.quiet_hours
        h = datetime.utcnow().hour
        if start <= end:
            return start <= h < end
        return h >= start or h < end

    def _allow_publish(self, task: dict) -> bool:
        if self._in_quiet_hours():
            logger.info("Тихие часы, задача пропущена: %s", task)
            metrics.tasks_skipped.labels(reason="quiet_hours").inc()
            return False
        if self.max_pages is not None and self.pages_sent >= self.max_pages:
            logger.warning(
                "Превышен лимит страниц %s, задача пропущена: %s", self.max_pages, task
            )
            metrics.budget_exceeded.labels(type="pages").inc()
            metrics.tasks_skipped.labels(reason="max_pages").inc()
            return False
        if self.max_tasks is not None and self.tasks_sent >= self.max_tasks:
            logger.warning(
                "Превышен лимит задач %s, задача пропущена: %s", self.max_tasks, task
            )
            metrics.budget_exceeded.labels(type="tasks").inc()
            metrics.tasks_skipped.labels(reason="max_tasks").inc()
            return False
        self.pages_sent += 1
        self.tasks_sent += 1
        return True

    async def _run_presets(self, notify: bool):
        self.pages_sent = 0
        self.tasks_sent = 0

        now = datetime.utcnow()
        async with SessionLocal() as session:
            res = await session.execute(
                select(User.geoid, User.filters_json, User.schedule_cron)
            )
            rows = res.all()

        pairs: set[tuple[str, str]] = set()
        all_categories = [
            item["name"].split(":")[0] for items in presets.sites.values() for item in items
        ]

        for geoid, filters_json, cron in rows:
            if cron:
                try:
                    trig = CronTrigger.from_crontab(cron)
                    if not trig.match(now):
                        logger.info(
                            "Пропуск геоида %s из-за расписания %s", geoid, cron
                        )
                        continue
                except Exception as e:
                    logger.warning(
                        "Некорректный cron %s для геоида %s: %s", cron, geoid, e
                    )
                    continue
            categories = []
            if filters_json and isinstance(filters_json, dict):
                categories = filters_json.get("categories") or []
            if not categories:
                categories = all_categories
            for cat in categories:
                pairs.add((cat, geoid))

        default_geoid = presets.geoid_default or settings.DEFAULT_GEOID
        for cat in all_categories:
            pairs.add((cat, default_geoid))

        min_discount = settings.MIN_DISCOUNT
        min_score = settings.MIN_SCORE

        for category, geoid in pairs:
            for site, items in presets.sites.items():
                for item in items:
                    item_cat = item["name"].split(":")[0]
                    if item_cat != category:
                        continue
                    task = {
                        "site": site,
                        "url": item["url"],
                        "geoid": geoid,
                        "category": category,
                        "min_discount": min_discount,
                        "min_score": min_score,
                        "notify": notify,
                    }
                    if not self._allow_publish(task):
                        continue
                    await self.queue.publish(task)
                    await asyncio.sleep(1.0)
