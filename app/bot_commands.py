from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import select

from .db import SessionLocal
from .models import User
from .policy import check_text, PolicyError

DEFAULT_CRON = "0 9,19 * * *"

router = Router()

async def _get_or_create_user(session, chat_id: int) -> User:
    res = await session.execute(select(User).where(User.chat_id == chat_id))
    user = res.scalar_one_or_none()
    if not user:
        user = User(chat_id=chat_id)
        session.add(user)
        await session.flush()
    return user

@router.message(Command("region"))
async def cmd_region(message: Message):
    try:
        check_text(message.text or "")
    except PolicyError as e:
        await message.answer(str(e))
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /region <geoid>")
        return
    geoid = parts[1]
    async with SessionLocal() as session:
        user = await _get_or_create_user(session, message.chat.id)
        user.geoid = geoid
        await session.commit()
    await message.answer(f"Регион установлен: {geoid}")

@router.message(Command("filters"))
async def cmd_filters(message: Message):
    try:
        check_text(message.text or "")
    except PolicyError as e:
        await message.answer(str(e))
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer("Использование: /filters <мин_скидка> <мин_скор>")
        return
    try:
        min_discount = int(parts[1])
        min_score = int(parts[2])
    except ValueError:
        await message.answer("Неверные значения")
        return
    async with SessionLocal() as session:
        user = await _get_or_create_user(session, message.chat.id)
        user.min_discount = min_discount
        user.min_score = min_score
        await session.commit()
    await message.answer("Фильтры обновлены")

@router.message(Command("pause"))
async def cmd_pause(message: Message):
    try:
        check_text(message.text or "")
    except PolicyError as e:
        await message.answer(str(e))
        return
    async with SessionLocal() as session:
        user = await _get_or_create_user(session, message.chat.id)
        user.schedule_cron = None
        await session.commit()
    await message.answer("Рассылка приостановлена")

@router.message(Command("resume"))
async def cmd_resume(message: Message):
    try:
        check_text(message.text or "")
    except PolicyError as e:
        await message.answer(str(e))
        return
    async with SessionLocal() as session:
        user = await _get_or_create_user(session, message.chat.id)
        user.schedule_cron = DEFAULT_CRON
        await session.commit()
    await message.answer("Рассылка возобновлена")
