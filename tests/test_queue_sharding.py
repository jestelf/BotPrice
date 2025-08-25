import pytest

from app.queue.backend import RedisQueue


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
    data = {
        "site": "ozon",
        "geoid": "213",
        "category": "phones",
        "url_template": "u",
        "page": 1,
    }
    await q.publish(data)
    assert q.redis.last_stream == "presets:ozon:213:phones"
