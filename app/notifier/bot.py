import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

async def send_batch(bot_token: str, chat_id: int, items: list[dict], chunk_size: int = 10):
    bot = Bot(bot_token)
    try:
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i+chunk_size]
            text_lines = []
            for idx, it in enumerate(chunk, 1):
                line = f"{idx+i}. {it['title']}\n" \
                       f"Цена: {it.get('price','—')} ₽" + \
                       (f" (−{it['discount_pct']}%)" if it.get('discount_pct') else "") + \
                       f"\nИсточник: {it['source']}\n{it['url']}\n"
                text_lines.append(line)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="★ В избранное", callback_data=f"fav:{k}")]
                for k in range(len(chunk))
            ])
            await bot.send_message(chat_id=chat_id, text="\n".join(text_lines), reply_markup=kb, disable_web_page_preview=False)
            await asyncio.sleep(0.7)
    finally:
        await bot.session.close()
