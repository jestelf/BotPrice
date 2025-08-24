import asyncio
from .queue import RedisQueue, PermanentError
from .processing.pipeline import process_preset
from .scraper.render import RenderService
from .db import SessionLocal
from .config import settings
from .notifier.bot import send_batch

import sentry_sdk
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
            results = await process_preset(
                session,
                self.render,
                site,
                task.get("url", ""),
                task.get("geoid") or None,
                int(task.get("min_discount", settings.MIN_DISCOUNT)),
                int(task.get("min_score", settings.MIN_SCORE)),
                task.get("weights"),
            )
        notify = task.get("notify") in {"True", True}
        if notify and settings.TG_CHAT_ID and results:
            best = sorted(results, key=lambda x: x["score"], reverse=True)[:20]
            await send_batch(settings.TELEGRAM_BOT_TOKEN, settings.TG_CHAT_ID, best)


async def main():
    if settings.SENTRY_DSN:
        sentry_sdk.init(settings.SENTRY_DSN)
    if settings.METRICS_PORT:
        start_http_server(settings.METRICS_PORT)
    queue = RedisQueue(settings.REDIS_URL, settings.QUEUE_STREAM)
    worker = Worker(queue)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
