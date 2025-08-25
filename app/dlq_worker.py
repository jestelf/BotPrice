import asyncio
import logging
import os

from prometheus_client import start_http_server

from .queue import RedisQueue
from .config import settings
from observability.logging import setup_logging


logger = logging.getLogger(__name__)


class DLQWorker:
    def __init__(self, queue: RedisQueue, shard: tuple[str | None, str | None, str | None] | None = None):
        self.queue = queue
        self.shard = shard

    async def start(self) -> None:
        site, geoid, category = self.shard if self.shard else (None, None, None)
        await self.queue.consume_dlq(self.handle_task, site=site, geoid=geoid, category=category)

    async def handle_task(self, task: dict) -> None:
        logger.info("DLQ task: %s", task)


async def main() -> None:
    setup_logging()
    if settings.METRICS_PORT:
        start_http_server(settings.METRICS_PORT)
    queue = RedisQueue(settings.REDIS_URL, settings.QUEUE_STREAM)
    shard = (
        os.getenv("WORKER_SITE"),
        os.getenv("WORKER_GEOID"),
        os.getenv("WORKER_CATEGORY"),
    )
    if all(v is None for v in shard):
        shard = None
    worker = DLQWorker(queue, shard=shard)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())

