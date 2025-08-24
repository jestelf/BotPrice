import asyncio
import random
from typing import Callable, Awaitable, Any

import redis.asyncio as redis
from redis.exceptions import ResponseError


class PermanentError(Exception):
    """Исключение для ошибок, которые не стоит ретраить."""
    pass


class AbstractQueue:
    async def publish(self, data: dict, dlq: bool = False) -> None:
        raise NotImplementedError

    async def consume(self, handler: Callable[[dict], Awaitable[Any]], consumer_name: str) -> None:
        raise NotImplementedError


class RedisQueue(AbstractQueue):
    def __init__(self, url: str, stream: str):
        self.redis = redis.from_url(url)
        self.stream = stream
        self.dlq_stream = f"{stream}:dlq"
        self.group = f"{stream}:group"
        self.dlq_group = f"{self.dlq_stream}:group"
        self.idempotency_ttl = 24 * 3600

    async def _ensure_group(self, stream: str, group: str) -> None:
        """Создаёт consumer group, если она ещё не существует."""
        try:
            await self.redis.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def publish(self, data: dict, dlq: bool = False) -> None:
        stream = self.dlq_stream if dlq else self.stream
        group = self.dlq_group if dlq else self.group
        await self._ensure_group(stream, group)

        site = data.get("site")
        url = data.get("url")
        page = data.get("page")
        geoid = data.get("geoid")
        idem_key = f"{site}:{url}:{page}:{geoid}"
        idem_redis_key = f"{stream}:idem:{idem_key}"
        added = await self.redis.setnx(idem_redis_key, 1)
        if not added:
            return
        await self.redis.expire(idem_redis_key, self.idempotency_ttl)

        payload = {k: str(v) for k, v in data.items()}
        payload["idempotency_key"] = idem_key
        await self.redis.xadd(stream, payload)

    async def consume(self, handler: Callable[[dict], Awaitable[Any]], consumer_name: str = "worker") -> None:
        await self._ensure_group(self.stream, self.group)
        max_retries = 5
        while True:
            res = await self.redis.xreadgroup(
                self.group, consumer_name, {self.stream: ">"}, count=1, block=1000
            )
            if not res:
                continue
            for _stream, messages in res:
                for msg_id, message in messages:
                    data = {k.decode(): v.decode() for k, v in message.items()}
                    retries = int(data.pop("retries", "0"))
                    try:
                        await handler(data)
                    except PermanentError:
                        await self.publish({**data, "retries": retries}, dlq=True)
                    except Exception as e:
                        status = getattr(e, "status", getattr(e, "status_code", None))
                        if status and 400 <= int(status) < 600:
                            await self.publish({**data, "retries": retries}, dlq=True)
                        elif retries + 1 >= max_retries:
                            await self.publish({**data, "retries": retries + 1}, dlq=True)
                        else:
                            backoff = (2 ** retries) + random.random()
                            await asyncio.sleep(backoff)
                            await self.publish({**data, "retries": retries + 1})
                    finally:
                        await self.redis.xack(self.stream, self.group, msg_id)
                        await self.redis.xdel(self.stream, msg_id)

    async def consume_dlq(
        self,
        handler: Callable[[dict], Awaitable[Any]],
        consumer_name: str = "dlq",
    ) -> None:
        from ..metrics import dlq_tasks_total, dlq_backlog
        from ..config import settings
        from ..notifier.monitoring import notify_monitoring

        await self._ensure_group(self.dlq_stream, self.dlq_group)
        threshold = getattr(settings, "DLQ_OVERFLOW_THRESHOLD", 100)

        while True:
            res = await self.redis.xreadgroup(
                self.dlq_group, consumer_name, {self.dlq_stream: ">"}, count=1, block=1000
            )
            if not res:
                backlog = await self.redis.xlen(self.dlq_stream)
                dlq_backlog.set(backlog)
                if backlog > threshold:
                    notify_monitoring(f"DLQ overflow: {backlog} messages")
                continue
            for _stream, messages in res:
                for msg_id, message in messages:
                    data = {k.decode(): v.decode() for k, v in message.items()}
                    try:
                        await handler(data)
                    finally:
                        dlq_tasks_total.inc()
                        await self.redis.xack(self.dlq_stream, self.dlq_group, msg_id)
                        await self.redis.xdel(self.dlq_stream, msg_id)
                        backlog = await self.redis.xlen(self.dlq_stream)
                        dlq_backlog.set(backlog)
                        if backlog > threshold:
                            notify_monitoring(f"DLQ overflow: {backlog} messages")
