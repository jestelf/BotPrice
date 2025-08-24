import types

import fakeredis.aioredis
import pytest
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def setup_module(module):
    import os
    os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")


class DummySession:
    async def close(self):  # pragma: no cover - simple stub
        pass


class DummyBot:
    def __init__(self, token):
        self.sent: list[str] = []

    async def send_message(self, chat_id, text, reply_markup=None, disable_web_page_preview=False):
        self.sent.append(text)

    @property
    def session(self):
        return DummySession()


@pytest.mark.asyncio
async def test_duplicate_suppression(monkeypatch):
    import types, sys

    aiogram_stub = types.ModuleType("aiogram")
    aiogram_stub.Bot = DummyBot
    class DummyButton:
        def __init__(self, text, callback_data):
            pass

    class DummyMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    types_stub = types.SimpleNamespace(
        InlineKeyboardMarkup=DummyMarkup, InlineKeyboardButton=DummyButton
    )
    sys.modules.setdefault("aiogram", aiogram_stub)
    sys.modules.setdefault("aiogram.types", types_stub)

    config_stub = types.ModuleType("app.config")
    config_stub.settings = types.SimpleNamespace(
        REDIS_URL="redis://localhost/0", DAILY_MSG_LIMIT=100
    )
    sys.modules.setdefault("app.config", config_stub)

    import app.notifier.bot as bot_mod

    r = fakeredis.aioredis.FakeRedis()
    monkeypatch.setattr(bot_mod, "_redis", lambda: r)

    sent = []

    class BotStub(DummyBot):
        async def send_message(self, *a, **k):
            await super().send_message(*a, **k)
            sent.append(1)

    monkeypatch.setattr(bot_mod, "Bot", BotStub)

    item = {"title": "t1", "url": "u1", "price": 1, "discount_pct": None, "source": "s"}

    await bot_mod.send_batch("token", 1, [item], chunk_size=1)
    assert len(sent) == 1

    # повторная отправка того же товара
    await bot_mod.send_batch("token", 1, [item], chunk_size=1)
    assert len(sent) == 1  # не увеличилось


@pytest.mark.asyncio
async def test_daily_limit(monkeypatch):
    import types, sys

    aiogram_stub = types.ModuleType("aiogram")
    aiogram_stub.Bot = DummyBot
    class DummyButton:
        def __init__(self, text, callback_data):
            pass

    class DummyMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    types_stub = types.SimpleNamespace(
        InlineKeyboardMarkup=DummyMarkup, InlineKeyboardButton=DummyButton
    )
    sys.modules.setdefault("aiogram", aiogram_stub)
    sys.modules.setdefault("aiogram.types", types_stub)

    config_stub = types.ModuleType("app.config")
    config_stub.settings = types.SimpleNamespace(
        REDIS_URL="redis://localhost/0", DAILY_MSG_LIMIT=2
    )
    sys.modules.setdefault("app.config", config_stub)

    import app.notifier.bot as bot_mod

    r = fakeredis.aioredis.FakeRedis()
    monkeypatch.setattr(bot_mod, "_redis", lambda: r)
    monkeypatch.setattr(bot_mod.settings, "DAILY_MSG_LIMIT", 2)

    sent = []

    class BotStub(DummyBot):
        async def send_message(self, *a, **k):
            await super().send_message(*a, **k)
            sent.append(1)

    monkeypatch.setattr(bot_mod, "Bot", BotStub)

    items = [
        {"title": f"t{i}", "url": f"u{i}", "price": i, "discount_pct": None, "source": "s"}
        for i in range(3)
    ]

    for it in items:
        await bot_mod.send_batch("token", 5, [it], chunk_size=1)

    assert len(sent) == 2  # третий не ушёл
    assert await r.sismember("cooldown:user", 5)

