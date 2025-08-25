import asyncio
import random
import difflib
import time
from typing import Dict
from urllib.parse import urlparse
from urllib import robotparser

import aiohttp

from .cache import ListingTTLCache


class Fetcher:
    """Асинхронный загрузчик HTML c ограничением параллельности и кэшем."""

    def __init__(self, user_agent: str = "BotPriceFetcher", per_domain: int = 2) -> None:
        self._ua = user_agent
        self._session = aiohttp.ClientSession(headers={"User-Agent": user_agent})
        self._per_domain = per_domain
        self._locks: Dict[str, asyncio.Semaphore] = {}
        self._robots: Dict[str, tuple[robotparser.RobotFileParser, float]] = {}
        self._cache = ListingTTLCache()
        self._errors: Dict[str, int] = {}

    async def close(self) -> None:
        await self._session.close()

    async def _robots_ok(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc
        rp, ts = self._robots.get(domain, (None, 0))
        if not rp or ts + 86400 < time.time():
            rp = robotparser.RobotFileParser()
            try:
                robots_url = f"{parsed.scheme}://{domain}/robots.txt"
                async with self._session.get(robots_url) as resp:
                    text = await resp.text()
            except Exception:
                text = ""
            rp.parse(text.splitlines())
            self._robots[domain] = (rp, time.time())
        return rp.can_fetch(self._ua, parsed.path or "/")

    async def fetch(self, url: str, *, max_attempts: int = 5) -> str:
        """Загружает страницу, учитывая роботов и бэкофф."""
        parsed = urlparse(url)
        domain = parsed.netloc
        if not await self._robots_ok(url):
            raise PermissionError("robots.txt запрещает доступ")
        sem = self._locks.setdefault(domain, asyncio.Semaphore(self._per_domain))
        err = self._errors.get(domain, 0)
        delay = random.uniform(0.5, 1.5) + err
        await asyncio.sleep(delay)
        cached = self._cache.get(url)
        async with sem:
            for attempt in range(max_attempts):
                try:
                    async with self._session.get(url) as resp:
                        status = resp.status
                        text = await resp.text()
                        if status in (403, 429):
                            err = self._errors.get(domain, 0) + 1
                            self._errors[domain] = err
                            backoff = min(30, 2 ** err) + random.random()
                            await asyncio.sleep(backoff)
                            continue
                        if cached:
                            ratio = difflib.SequenceMatcher(None, cached, text).ratio()
                            if ratio >= 0.9:
                                self._cache.set(url, cached)
                                self._errors[domain] = 0
                                return cached
                        self._cache.set(url, text)
                        self._errors[domain] = 0
                        return text
                except Exception:
                    err = self._errors.get(domain, 0) + 1
                    self._errors[domain] = err
                    await asyncio.sleep(min(30, 2 ** err))
            raise RuntimeError("Не удалось получить страницу")


__all__ = ["Fetcher"]

