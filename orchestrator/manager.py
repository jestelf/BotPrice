from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections import defaultdict
from datetime import date
from typing import Dict
from urllib.parse import urlparse

class Manager:
    """Управляет дневным лимитом страниц и параллельностью по доменам."""

    def __init__(
        self,
        daily_page_limit: int | None = None,
        domain_limits: Dict[str, int] | None = None,
    ) -> None:
        self.daily_page_limit = daily_page_limit
        self.domain_limits = domain_limits or {}
        self._day = date.today()
        self._pages_today = 0
        self._inflight: Dict[str, int] = defaultdict(int)
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    def _reset_day(self) -> None:
        today = date.today()
        if today != self._day:
            self._day = today
            self._pages_today = 0
            self._inflight.clear()

    def _has_budget(self) -> bool:
        self._reset_day()
        if self.daily_page_limit is None:
            return True
        return self._pages_today < self.daily_page_limit

    def reserve(self, domain: str) -> bool:
        """Резервирует бюджет для страницы указанного домена."""
        self._reset_day()
        if not self._has_budget():
            return False
        limit = self.domain_limits.get(domain)
        if limit is not None and self._inflight[domain] >= limit:
            return False
        self._inflight[domain] += 1
        self._pages_today += 1
        return True

    def release(self, domain: str) -> None:
        if domain in self._inflight and self._inflight[domain] > 0:
            self._inflight[domain] -= 1
            if self._inflight[domain] <= 0:
                self._inflight.pop(domain, None)

    def _get_semaphore(self, domain: str) -> asyncio.Semaphore:
        limit = self.domain_limits.get(domain, 1)
        sem = self._semaphores.get(domain)
        if sem is None:
            sem = asyncio.Semaphore(limit)
            self._semaphores[domain] = sem
        return sem

    @asynccontextmanager
    async def limit(self, domain: str):
        """Асинхронный контекст для выполнения страницы."""
        if not self.reserve(domain):
            raise RuntimeError("budget exceeded")
        sem = self._get_semaphore(domain)
        await sem.acquire()
        try:
            yield
        finally:
            sem.release()
            self.release(domain)

async def create_preset_tasks(queue, manager: Manager, tasks: list[dict]) -> None:
    """Публикует задачи, проверяя остаток бюджета перед отправкой."""
    for task in tasks:
        url = task.get("url", "")
        domain = urlparse(url).netloc
        if not manager.reserve(domain):
            continue
        await queue.publish(task)
