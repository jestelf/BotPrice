import os
import types
import asyncio
import sys
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Ensure settings env var
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.models import Base, User
from app.queue import AbstractQueue
from pydantic import BaseModel
import app.orchestrator as orchestrator
from app.orchestrator import Orchestrator
from app import metrics

class DummyQueue(AbstractQueue):
    def __init__(self):
        self.tasks = []
    async def publish(self, data, dlq: bool = False) -> None:
        if isinstance(data, BaseModel):
            self.tasks.append(data.model_dump())
        else:
            self.tasks.append(data)


@pytest.mark.asyncio
async def test_run_presets_by_category_and_geoid(monkeypatch):
    # in-memory DB
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with async_session() as session:
        session.add_all([
            User(chat_id=1, geoid="1", filters_json={"categories": ["Ноутбуки", "Смартфоны"]}, schedule_cron=None),
            User(chat_id=2, geoid="2", filters_json={"categories": ["Смартфоны"]}, schedule_cron="0 0 1 1 *"),
        ])
        await session.commit()
    monkeypatch.setattr(orchestrator, "SessionLocal", async_session)

    presets_stub = types.SimpleNamespace(
        geoid_default=None,
        sites={
            "ozon": [
                {"name": "Ноутбуки: скидки", "url": "ozon/n"},
                {"name": "Смартфоны: скидки", "url": "ozon/s"},
            ],
            "market": [
                {"name": "Ноутбуки: скидки", "url": "market/n"},
                {"name": "Смартфоны: скидки", "url": "market/s"},
            ],
        },
    )
    monkeypatch.setattr(orchestrator, "presets", presets_stub)
    monkeypatch.setattr(orchestrator.settings, "DEFAULT_GEOID", "1")

    async def dummy_sleep(_):
        pass
    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)

    q = DummyQueue()
    orch = Orchestrator(q)
    await orch._run_presets(True)

    assert len(q.tasks) == 4
    assert all(t["geoid"] == "1" for t in q.tasks)
    urls = {t["url"] for t in q.tasks}
    assert urls == {"ozon/n", "ozon/s", "market/n", "market/s"}


@pytest.mark.asyncio
async def test_budget_metrics(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with async_session() as session:
        session.add(User(chat_id=1, geoid="1", filters_json={"categories": ["Ноутбуки"]}, schedule_cron=None))
        await session.commit()
    monkeypatch.setattr(orchestrator, "SessionLocal", async_session)

    presets_stub = types.SimpleNamespace(
        geoid_default=None,
        sites={
            "ozon": [{"name": "Ноутбуки: скидки", "url": "ozon/n"}],
            "market": [{"name": "Ноутбуки: скидки", "url": "market/n"}],
        },
    )
    monkeypatch.setattr(orchestrator, "presets", presets_stub)
    monkeypatch.setattr(orchestrator.settings, "DEFAULT_GEOID", "1")

    async def dummy_sleep(_):
        pass
    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)

    metrics.budget_exceeded.labels(type="pages")._value.set(0)
    metrics.tasks_skipped.labels(reason="max_pages")._value.set(0)

    q = DummyQueue()
    orch = Orchestrator(q, max_pages=1, max_tasks=2)
    await orch._run_presets(True)

    assert len(q.tasks) == 1
    assert metrics.budget_exceeded.labels(type="pages")._value.get() == 1
    assert metrics.tasks_skipped.labels(reason="max_pages")._value.get() == 1
