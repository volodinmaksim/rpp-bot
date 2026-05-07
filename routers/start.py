from contextlib import suppress

from aiogram import F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import settings
from data.states import StoryState
from data.story_content import (
    text_advertising_consent,
    text_hello,
    text_subscription_is_confirmed,
)
from db.crud import add_event, add_user
from exception.db import UserNotFound
from loader import logger
from utils.scheduler import clear_user_story_jobs

router = Router(name="start_router")


def get_subscription_channels() -> tuple[tuple[int, str], ...]:
    return (
        (settings.CHAT_ID_TO_CHECK, settings.CHAT_URL),
        (settings.SECOND_CHAT_ID_TO_CHECK, settings.SECOND_CHAT_URL),
    )


def build_subscription_keyboard() -> types.InlineKeyboardMarkup:
    return build_subscription_check_keyboard(check_callback_data="check_sub")


def build_subscription_check_keyboard(
    *, check_callback_data: str
) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, (_, url) in enumerate(get_subscription_channels(), start=1):
        builder.button(text=f"{index}. Подписаться", url=url)
    builder.button(
        text=f"{len(get_subscription_channels()) + 1}. Я подписалась!",
        callback_data=check_callback_data,
    )
    builder.adjust(1)
    return builder.as_markup()


async def is_subscribed_to_all_channels(user_id: int) -> bool:
    from loader import bot

    subscriptions = [
        await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        for chat_id, _ in get_subscription_channels()
    ]
    return all(
        user_sub.status in ["member", "administrator", "creator"]
        for user_sub in subscriptions
    )


async def add_event_safely(*, tg_id: int, event_name: str) -> None:
    try:
        await add_event(tg_id=tg_id, event_name=event_name)
    except UserNotFound:
        logger.error("Ошибка: пользователь с tg_id %s не найден в базе.", tg_id)


async def send_questionnaire_link(
    callback: types.CallbackQuery, state: FSMContext
) -> None:
    tg_id = callback.from_user.id
    clear_user_story_jobs(tg_id=tg_id)
    await add_event_safely(
        tg_id=tg_id,
        event_name='Получить файл: "Пенсильванский опросник"',
    )

    await callback.message.edit_text(f"Пенсильванский опросник:\n{settings.YDISK_LINK}")
    await state.set_state(StoryState.final_stage)


async def send_subscription_confirmed_message(message: types.Message) -> None:
    builder = InlineKeyboardBuilder()
    builder.button(text="Получить опросник 📥", callback_data="track_link_click")

    await message.answer(
        text_subscription_is_confirmed,
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )


@router.message(Command("start"))
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext):
    await state.set_state(StoryState.waiting_for_advertising_consent)

    clear_user_story_jobs(tg_id=message.from_user.id)
    utm = (command.args or "").strip()
    user_name = (
        message.from_user.username
        or f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
    )
    await add_user(tg_id=message.from_user.id, username=user_name, utm_mark=utm)

    builder = InlineKeyboardBuilder()
    builder.button(text="Разрешаю", callback_data="allow_advertising")
    builder.adjust(1)

    await message.answer(
        text_advertising_consent,
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )


@router.callback_query(
    StoryState.waiting_for_advertising_consent, F.data == "allow_advertising"
)
async def accept_advertising(callback: types.CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await callback.answer()

    await add_event_safely(
        tg_id=callback.from_user.id,
        event_name="advertising_consent",
    )
    await state.set_state(StoryState.waiting_for_subscription)
    await callback.message.answer(
        text_hello,
        reply_markup=build_subscription_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(StoryState.waiting_for_subscription, F.data == "check_sub")
async def verify_subscription(callback: types.CallbackQuery, state: FSMContext):
    from loader import bot

    with suppress(TelegramBadRequest):
        await callback.answer()

    if await is_subscribed_to_all_channels(callback.from_user.id):
        await state.set_state(StoryState.waiting_15min_pause)
        await send_subscription_confirmed_message(callback.message)
        return

    await callback.message.answer("Вы еще не подписались на все каналы!")


@router.callback_query(F.data == "check_extra_yes_sub")
async def verify_extra_yes_broadcast_subscription(
    callback: types.CallbackQuery, state: FSMContext
):
    with suppress(TelegramBadRequest):
        await callback.answer()

    await add_event_safely(
        tg_id=callback.from_user.id,
        event_name="click_check_extra_yes_subscription",
    )

    if await is_subscribed_to_all_channels(callback.from_user.id):
        await send_subscription_confirmed_message(callback.message)
        return

    await callback.message.answer("Вы еще не подписались на все каналы!")


@router.callback_query(F.data == "track_link_click")
async def track_link_click(callback: types.CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await callback.answer()

    await add_event_safely(
        tg_id=callback.from_user.id,
        event_name="click_get_questionnaire",
    )
    await send_questionnaire_link(callback, state)
