import asyncio
from typing import Callable, Awaitable, Any
import redis.asyncio as redis


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
        self.last_id = "0-0"

    async def publish(self, data: dict, dlq: bool = False) -> None:
        stream = self.dlq_stream if dlq else self.stream
        # redis streams хранят строки
        payload = {k: str(v) for k, v in data.items()}
        await self.redis.xadd(stream, payload)

    async def consume(self, handler: Callable[[dict], Awaitable[Any]], consumer_name: str = "worker") -> None:
        max_retries = 5
        while True:
            res = await self.redis.xread({self.stream: self.last_id}, block=1000, count=1)
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
                    except Exception:
                        if retries + 1 >= max_retries:
                            await self.publish({**data, "retries": retries + 1}, dlq=True)
                        else:
                            await asyncio.sleep(2 ** retries)
                            await self.publish({**data, "retries": retries + 1})
                    finally:
                        await self.redis.xdel(self.stream, msg_id)
                        self.last_id = msg_id
