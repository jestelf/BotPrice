import random
import time
from typing import Any, Dict, Tuple


class ListingTTLCache:
    """Простой TTL-кэш для HTML листингов по URL."""

    def __init__(self, ttl_min: int = 30, ttl_max: int = 180) -> None:
        if ttl_min > ttl_max:
            raise ValueError("ttl_min должен быть меньше или равен ttl_max")
        self.ttl_min = ttl_min
        self.ttl_max = ttl_max
        self._store: Dict[str, Tuple[Any, float]] = {}

    def get(self, url: str) -> Any | None:
        """Возвращает значение из кэша, если оно не просрочено."""
        entry = self._store.get(url)
        if not entry:
            return None
        value, expires_at = entry
        if expires_at < time.time():
            del self._store[url]
            return None
        return value

    def set(self, url: str, value: Any) -> None:
        """Сохраняет значение в кэш с произвольным TTL."""
        ttl = random.randint(self.ttl_min, self.ttl_max)
        self._store[url] = (value, time.time() + ttl)

    def clear(self) -> None:
        """Очищает кэш."""
        self._store.clear()
