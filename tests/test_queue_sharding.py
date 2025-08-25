import pytest

from app.queue.backend import RedisQueue
from app.schemas import TaskPayload


class DummyRedis:
    def __init__(self):
        self.last_stream = None

    async def xgroup_create(self, *args, **kwargs):
        pass

    async def setnx(self, *args, **kwargs):
        return True

    async def expire(self, *args, **kwargs):
        pass

    async def xadd(self, stream, payload):
        self.last_stream = stream


@pytest.mark.asyncio
async def test_publish_shards():
    q = RedisQueue("redis://localhost", "presets")
    q.redis = DummyRedis()
    data = TaskPayload(
        site="ozon",
        url="https://example.com",
        geoid="213",
        category="phones",
        min_discount=0,
        min_score=0,
        url_template="u",
        page=1,
    )
    await q.publish(data)
    assert q.redis.last_stream == "presets:ozon:213:phones"
