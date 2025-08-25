import asyncio
import hashlib
import redis.asyncio as redis
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from ..config import settings


def _redis() -> redis.Redis:
    return redis.from_url(settings.REDIS_URL)


async def send_batch(bot_token: str, chat_id: int, items: list[dict], chunk_size: int = 10):
    r = _redis()
    bot = Bot(bot_token)
    try:
        # проверка лимита пользователя
        user_key = f"cooldown:user:{chat_id}"
        if await r.exists(user_key):
            return
        count_key = f"msgcount:{chat_id}"
        current = int(await r.get(count_key) or 0)
        if current >= settings.DAILY_MSG_LIMIT:
            await r.setex(user_key, 24 * 3600, 1)
            return

        remaining = settings.DAILY_MSG_LIMIT - current

        to_send: list[dict] = []
        prod_key = f"cooldown:product:{chat_id}"
        for it in items:
            pid = hashlib.md5(it["url"].encode()).hexdigest()
            if await r.sismember(prod_key, pid):
                continue
            await r.sadd(prod_key, pid)
            await r.expire(prod_key, 48 * 3600)
            to_send.append(it)
            if len(to_send) >= remaining:
                break

        if not to_send:
            return

        await r.incrby(count_key, len(to_send))
        await r.expire(count_key, 24 * 3600)
        if current + len(to_send) >= settings.DAILY_MSG_LIMIT:
            await r.setex(user_key, 24 * 3600, 1)

        for i in range(0, len(to_send), chunk_size):
            chunk = to_send[i:i + chunk_size]
            text_lines = []
            for idx, it in enumerate(chunk, 1):
                line = (
                    f"{idx + i}. {it['title']}\n"
                    f"Цена: {it.get('price', '—')} ₽"
                    + (f" (−{it['discount_pct']}%)" if it.get('discount_pct') else "")
                    + f"\nИсточник: {it['source']}\n{it['url']}\n"
                )
                text_lines.append(line)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="★", callback_data=f"fav:{k}"),
                        InlineKeyboardButton(text="Скрыть", callback_data=f"hide:{k}"),
                        InlineKeyboardButton(text="Ещё −10%", callback_data=f"more:{k}"),
                    ]
                    for k in range(len(chunk))
                ]
            )
            await bot.send_message(
                chat_id=chat_id,
                text="\n".join(text_lines),
                reply_markup=kb,
                disable_web_page_preview=False,
            )
            await asyncio.sleep(0.7)
    finally:
        await bot.session.close()
        await r.close()
