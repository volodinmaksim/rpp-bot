import asyncio
from contextlib import suppress
from datetime import datetime, timedelta

from aiogram import Router, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from data.states import StoryState
from db.crud import add_event, get_user
from data.story_content import (
    text_reviews,
    final_goodbye_text_down,
)
from loader import bot
from utils.scheduler import schedule_user_job
from utils.common import get_next_working_time

router = Router()


@router.callback_query(F.data == "survey_yes", StoryState.waiting_for_survey_response)
async def process_survey_yes(callback: types.CallbackQuery, state: FSMContext):
    user = await get_user(tg_id=callback.from_user.id)

    await state.set_state(StoryState.waiting_for_wishes)

    with suppress(TelegramBadRequest):
        await callback.message.edit_reply_markup(reply_markup=None)

    await callback.message.edit_text(
        "Здорово, что информация была полезной!\n\n"
        "Мы регулярно добавляем новый контент. Поделитесь пожалуйста, какие темы вам интересны?\n\n"
        "Будем учитывать ваши пожелания при подготовке материалов.\n\n"
        "P.S. А новый полезный текст пришлем завтра."
    )

    if user and user.segment == "pro":
        from routers.pro_continued import send_pro_text_10

        target_func = send_pro_text_10
    else:
        from routers.novice_continued import send_novice_text_4

        target_func = send_novice_text_4

    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"continued_path:{callback.from_user.id}",
        run_date=run_date,
        func=target_func,
        args=[callback.message.chat.id],
    )
    await callback.answer()


@router.callback_query(F.data == "survey_no", StoryState.waiting_for_survey_response)
async def process_survey_no(callback: types.CallbackQuery, state: FSMContext):
    # await add_event(tg_id=callback.from_user.id, event_name="survey_answered_no")

    await callback.message.edit_text(
        text="Эх, тогда вот последний текст и прощаемся ☺️",
        parse_mode="HTML",
    )
    await asyncio.sleep(1.5)

    builder = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Я передумал, хочу продолжить! ⚡️",
                    callback_data="decided_continue",
                )
            ]
        ]
    )

    photo = FSInputFile("data/photos/text_final.jpg")
    await bot.send_photo(
        chat_id=callback.from_user.id,
        photo=photo,
        caption=final_goodbye_text_down,
        parse_mode="HTML",
        reply_markup=builder,
    )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "decided_continue")
async def user_come_back(callback: types.CallbackQuery, state: FSMContext):

    await callback.answer()

    user = await get_user(tg_id=callback.from_user.id)

    await callback.message.answer(
        text="<b>Отлично, продолжаем!</b> \n\nА вот следующий материал.",
        parse_mode="HTML",
    )

    # 3. Направляем в нужную ветку
    if user and user.segment == "pro":
        from routers.pro_continued import send_pro_text_10

        await send_pro_text_10(chat_id=callback.from_user.id)
    else:
        from routers.novice_continued import send_novice_text_4

        await send_novice_text_4(chat_id=callback.from_user.id)


@router.message(StoryState.waiting_for_wishes)
async def collect_user_wishes(message: types.Message, state: FSMContext):
    await add_event(
        tg_id=message.from_user.id,
        event_name=f"user_wish: {(message.text or '').strip()}",
    )

    await message.answer(
        "Спасибо большое! Нам понадобится время, чтобы обработать ваш запрос.\n\n"
        "Возможно часть запросов будет публиковаться в канале Школы ЧК."
    )

    await state.set_state(StoryState.final_stage)


@router.callback_query(F.data == "show_reviews")
async def process_show_reviews(callback: types.CallbackQuery):
    await callback.answer()
    with suppress(Exception):
        await callback.message.edit_reply_markup(reply_markup=None)

    photo_path = FSInputFile("data/photos/meme1.png")
    await bot.send_photo(
        chat_id=callback.from_user.id,
        photo=photo_path,
        caption="Чего-чего, а отзывов у нас очень много!",
    )

    await asyncio.sleep(1.5)
    await callback.message.answer(text_reviews, parse_mode="HTML")
