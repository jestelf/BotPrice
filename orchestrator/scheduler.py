from __future__ import annotations

from datetime import datetime
from typing import Iterable

from apscheduler.triggers.cron import CronTrigger

class Scheduler:
    """Фильтр задач по тихим часам."""

    def __init__(self, quiet_crons: Iterable[str] | None = None) -> None:
        self.quiet_triggers = []
        for cron in quiet_crons or []:
            try:
                self.quiet_triggers.append(CronTrigger.from_crontab(cron))
            except Exception:
                continue

    def is_quiet(self, now: datetime | None = None) -> bool:
        now = now or datetime.utcnow()
        return any(trig.match(now) for trig in self.quiet_triggers)

    def allow(self, now: datetime | None = None) -> bool:
        """Возвращает True, если задачи можно выполнять."""
        return not self.is_quiet(now)
