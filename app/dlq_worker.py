import asyncio
import logging

from prometheus_client import start_http_server

from .queue import RedisQueue
from .config import settings
from observability.logging import setup_logging


logger = logging.getLogger(__name__)


class DLQWorker:
    def __init__(self, queue: RedisQueue):
        self.queue = queue

    async def start(self) -> None:
        await self.queue.consume_dlq(self.handle_task)

    async def handle_task(self, task: dict) -> None:
        logger.info("DLQ task: %s", task)


async def main() -> None:
    setup_logging()
    if settings.METRICS_PORT:
        start_http_server(settings.METRICS_PORT)
    queue = RedisQueue(settings.REDIS_URL, settings.QUEUE_STREAM)
    worker = DLQWorker(queue)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())

