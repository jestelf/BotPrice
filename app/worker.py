import asyncio
from sqlalchemy import select

from .queue import RedisQueue, PermanentError
from .processing.pipeline import process_preset
from .scraper.render import RenderService
from .db import SessionLocal
from .config import settings
from .notifier.bot import send_batch
from .models import User
from observability.logging import setup_logging
from prometheus_client import start_http_server


class Worker:
    def __init__(self, queue: RedisQueue):
        self.queue = queue
        self.render = RenderService()

    async def start(self):
        await self.render.start()
        await self.queue.consume(self.handle_task)

    async def handle_task(self, task: dict):
        site = task.get("site")
        if site not in {"ozon", "market"}:
            raise PermanentError(f"Unknown site {site}")
        async with SessionLocal() as session:
            min_discount = int(task.get("min_discount", settings.MIN_DISCOUNT))
            min_score = int(task.get("min_score", settings.MIN_SCORE))
            weights = task.get("weights")
            geoid = task.get("geoid") or None

            chat_id = task.get("chat_id")
            if chat_id:
                res = await session.execute(select(User).where(User.chat_id == int(chat_id)))
                user = res.scalar_one_or_none()
                if user:
                    geoid = geoid or user.geoid
                    min_discount = user.min_discount or min_discount
                    min_score = user.min_score or min_score
                    if user.score_weights_json:
                        weights = user.score_weights_json

            results = await process_preset(
                session,
                self.render,
                site,
                task.get("url", ""),
                geoid,
                min_discount,
                min_score,
                weights,
            )
        notify = task.get("notify") in {"True", True}
        if notify and settings.TG_CHAT_ID and results:
            best = sorted(results, key=lambda x: x["score"], reverse=True)[:20]
            await send_batch(settings.TELEGRAM_BOT_TOKEN, settings.TG_CHAT_ID, best)


async def main():
    setup_logging()
    if settings.METRICS_PORT:
        start_http_server(settings.METRICS_PORT)
    queue = RedisQueue(settings.REDIS_URL, settings.QUEUE_STREAM)
    worker = Worker(queue)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
