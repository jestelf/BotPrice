from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis


class RedisCache:
    """Кэш листингов и антидубликат на Redis."""

    def __init__(
        self,
        url: str,
        *,
        listing_prefix: str = "listing",
        dedup_key: str = "seen",
    ) -> None:
        self._redis = redis.from_url(url)
        self._listing_prefix = listing_prefix
        self._dedup_key = dedup_key

    def _listing_key(self, key: str) -> str:
        return f"{self._listing_prefix}:{key}"

    async def get_listing(self, key: str) -> Any | None:
        raw = await self._redis.get(self._listing_key(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw

    async def set_listing(self, key: str, value: Any, ttl: int) -> None:
        data: Any = value
        if not isinstance(value, (bytes, bytearray)):
            data = json.dumps(value)
        await self._redis.set(self._listing_key(key), data, ex=ttl)

    async def is_duplicate(self, key: str, ttl: int) -> bool:
        """Возвращает True, если ключ уже встречался."""
        added = await self._redis.sadd(self._dedup_key, key)
        if added and ttl:
            await self._redis.expire(self._dedup_key, ttl)
        return added == 0

    async def close(self) -> None:
        await self._redis.close()
