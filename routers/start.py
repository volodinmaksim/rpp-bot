from datetime import datetime, timedelta

from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import Router, types, F
from config import settings
from data.states import StoryState
from db.crud import add_user, add_event
from exception.db import UserNotFound
from loader import logger
from data.story_content import text_hello, text_subscription_is_confirmed
from utils.scheduler import clear_user_story_jobs, send_15min_survey, schedule_user_job

router = Router(name="start_router")


@router.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext):
    await state.set_state(StoryState.waiting_for_subscription)

    clear_user_story_jobs(tg_id=message.from_user.id)
    utm = (command.args or "").strip()
    user_name = (
        message.from_user.username
        or f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
    )
    await add_user(tg_id=message.from_user.id, username=user_name, utm_mark=utm)

    builder = InlineKeyboardBuilder()
    builder.button(text="1. Подписаться", url=settings.CHAT_URL)
    builder.button(text="2. Я подписался!", callback_data="check_sub")
    builder.adjust(1)

    await message.answer(text_hello, reply_markup=builder.as_markup())


@router.callback_query(StoryState.waiting_for_subscription, F.data == "check_sub")
async def verify_subscription(callback: types.CallbackQuery, state: FSMContext):
    from loader import bot

    user_sub = await bot.get_chat_member(
        chat_id=settings.CHAT_ID_TO_CHECK, user_id=callback.from_user.id
    )

    if user_sub.status in ["member", "administrator", "creator"]:
        await state.set_state(StoryState.waiting_15min_pause)

        builder = InlineKeyboardBuilder()
        builder.button(
            text="Скачать пакет инструментов 📥", callback_data="track_link_click"
        )

        await callback.message.answer(
            text_subscription_is_confirmed,
            reply_markup=builder.as_markup(),
        )
        schedule_user_job(
            job_id=f"15min_survey:{callback.from_user.id}",
            run_date=datetime.now() + timedelta(minutes=5),
            func=send_15min_survey,
            args=[callback.message.chat.id],
        )
    else:
        await callback.answer("Вы еще не подписались на канал!", show_alert=True)


@router.callback_query(F.data == "track_link_click")
async def track_link_click(callback: types.CallbackQuery):
    tg_id = callback.from_user.id
    try:
        await add_event(
            tg_id=tg_id,
            event_name='Получить файл: "Пакет инструментов для работы с РПП от Ирины Ушаковой"',
        )
    except UserNotFound:
        logger.error("Ошибка: пользователь с tg_id %s не найден в базе.", tg_id)

    await callback.message.edit_text(f"Ваша ссылка: {settings.YDISK_LINK}")
