import asyncio
import random
from typing import Callable, Awaitable, Any
import json
from pydantic import BaseModel
from ..schemas import TaskPayload

import redis.asyncio as redis
from redis.exceptions import ResponseError


class PermanentError(Exception):
    """Исключение для ошибок, которые не стоит ретраить."""
    pass


class AbstractQueue:
    async def publish(self, data: dict | BaseModel, dlq: bool = False) -> None:
        raise NotImplementedError

    async def consume(self, handler: Callable[[Any], Awaitable[Any]], consumer_name: str) -> None:
        raise NotImplementedError


class RedisQueue(AbstractQueue):
    def __init__(self, url: str, stream: str):
        self.redis = redis.from_url(url)
        self.stream = stream
        self.idempotency_ttl = 24 * 3600

    def _shard_stream(self, site: str | None, geoid: str | None, category: str | None) -> str:
        """Формирует имя потока с учётом шардов."""
        parts = [self.stream]
        if site is not None:
            parts.append(site)
            parts.append(geoid or "none")
            parts.append(category or "none")
        return ":".join(parts)

    async def _ensure_group(self, stream: str, group: str) -> None:
        """Создаёт consumer group, если она ещё не существует."""
        try:
            await self.redis.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def publish(self, data: dict | BaseModel, dlq: bool = False) -> None:
        retries = None
        if isinstance(data, BaseModel):
            model = TaskPayload.model_validate(data.model_dump())
        else:
            retries = data.get("retries")
            model = TaskPayload.model_validate(data)
        data_dict = model.model_dump()

        site = data_dict.get("site")
        geoid = data_dict.get("geoid")
        category = data_dict.get("category")
        base_stream = self._shard_stream(site, geoid, category)
        stream = f"{base_stream}:dlq" if dlq else base_stream
        group = f"{stream}:group"
        await self._ensure_group(stream, group)

        url_template = data_dict.get("url_template") or data_dict.get("url")
        page = data_dict.get("page")
        idem_key = f"{site}:{geoid}:{category}:{url_template}:{page}"
        idem_redis_key = f"{stream}:idem:{idem_key}"
        added = await self.redis.setnx(idem_redis_key, 1)
        if not added:
            return
        await self.redis.expire(idem_redis_key, self.idempotency_ttl)

        payload = {
            "data": json.dumps(data_dict, ensure_ascii=False),
            "idempotency_key": idem_key,
        }
        if retries is not None:
            payload["retries"] = str(retries)
        await self.redis.xadd(stream, payload)

    async def consume(
        self,
        handler: Callable[[dict], Awaitable[Any]],
        consumer_name: str = "worker",
        site: str | None = None,
        geoid: str | None = None,
        category: str | None = None,
    ) -> None:
        stream = self._shard_stream(site, geoid, category)
        group = f"{stream}:group"
        await self._ensure_group(stream, group)
        max_retries = 5
        while True:
            res = await self.redis.xreadgroup(
                group, consumer_name, {stream: ">"}, count=1, block=1000,
            )
            if not res:
                continue
            for _stream, messages in res:
                for msg_id, message in messages:
                    data = {k.decode(): v.decode() for k, v in message.items()}
                    retries = int(data.pop("retries", "0"))
                    task = TaskPayload.model_validate_json(data.get("data", "{}"))
                    try:
                        await handler(task)
                    except PermanentError:
                        await self.publish({**task.model_dump(), "retries": retries}, dlq=True)
                    except Exception as e:
                        status = getattr(e, "status", getattr(e, "status_code", None))
                        if status and 400 <= int(status) < 600:
                            await self.publish({**data, "retries": retries}, dlq=True)
                        elif retries + 1 >= max_retries:
                            await self.publish({**task.model_dump(), "retries": retries + 1}, dlq=True)
                        else:
                            backoff = (2 ** retries) + random.random()
                            await asyncio.sleep(backoff)
                            await self.publish({**task.model_dump(), "retries": retries + 1})
                    finally:
                        await self.redis.xack(stream, group, msg_id)
                        await self.redis.xdel(stream, msg_id)

    async def consume_dlq(
        self,
        handler: Callable[[dict], Awaitable[Any]],
        consumer_name: str = "dlq",
        site: str | None = None,
        geoid: str | None = None,
        category: str | None = None,
    ) -> None:
        from ..metrics import dlq_tasks_total, dlq_backlog
        from ..config import settings
        from ..notifier.monitoring import notify_monitoring

        base_stream = self._shard_stream(site, geoid, category)
        dlq_stream = f"{base_stream}:dlq"
        dlq_group = f"{dlq_stream}:group"

        await self._ensure_group(dlq_stream, dlq_group)
        threshold = getattr(settings, "DLQ_OVERFLOW_THRESHOLD", 100)

        while True:
            res = await self.redis.xreadgroup(
                dlq_group, consumer_name, {dlq_stream: ">"}, count=1, block=1000
            )
            if not res:
                backlog = await self.redis.xlen(dlq_stream)
                dlq_backlog.set(backlog)
                if backlog > threshold:
                    notify_monitoring(f"DLQ overflow: {backlog} messages")
                continue
            for _stream, messages in res:
                for msg_id, message in messages:
                    data = {k.decode(): v.decode() for k, v in message.items()}
                    task = TaskPayload.model_validate_json(data.get("data", "{}"))
                    try:
                        await handler(task)
                    finally:
                        dlq_tasks_total.inc()
                        await self.redis.xack(dlq_stream, dlq_group, msg_id)
                        await self.redis.xdel(dlq_stream, msg_id)
                        backlog = await self.redis.xlen(dlq_stream)
                        dlq_backlog.set(backlog)
                        if backlog > threshold:
                            notify_monitoring(f"DLQ overflow: {backlog} messages")
