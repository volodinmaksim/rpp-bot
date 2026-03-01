import asyncio
from contextlib import suppress
from datetime import datetime, timedelta

from aiogram import Router, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from data.states import StoryState
from data.story_content import (
    text_7_for_pro,
    text_8_for_pro,
    text_9_for_pro,
    survey_question_text,
    text_after_level,
)
from db.crud import set_segment
from loader import bot, dp
from utils.common import get_next_working_time, my_send_photos, my_send_text_and_photos
from utils.keyboards import get_feedback_kb, get_survey_kb
from utils.scheduler import schedule_user_job

router = Router()


async def send_pro_text_9(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.pro_path)

    photo = FSInputFile("data/photos/text_9_pro_1.jpg")

    await bot.send_photo(
        chat_id=chat_id,
        photo=photo,
    )
    await bot.send_message(
        chat_id=chat_id,
        text=text_9_for_pro,
        parse_mode="HTML",
        reply_markup=get_feedback_kb(post_id="9pro"),
    )

    await send_survey_after_pro(chat_id)


async def send_pro_text_8(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.pro_path)

    photos = [f"data/photos/text_8_pro_{i}.jpg" for i in range(1, 8)]
    await my_send_photos(
        chat_id=chat_id,
        text=text_8_for_pro,
        photos=photos,
        post_id="8pro",
    )

    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"pro_text_9:{chat_id}",
        run_date=run_date,
        func=send_pro_text_9,
        args=[chat_id],
    )


async def send_survey_after_pro(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.waiting_for_survey_response)

    await bot.send_message(
        chat_id=chat_id,
        text=survey_question_text,
        reply_markup=get_survey_kb(),
        parse_mode="HTML",
    )



@router.callback_query(F.data == "exp_pro", StoryState.choosing_experience)
async def start_pro_path(callback: types.CallbackQuery, state: FSMContext):

    with suppress(TelegramBadRequest):
        await callback.message.edit_text(text=text_after_level)

    tg_id = callback.from_user.id

    await set_segment(tg_id=tg_id, segment="pro")

    await asyncio.sleep(1.5)
    photos = [f"data/photos/text_7_pro_{i}.jpg" for i in range(1, 11)]
    await my_send_text_and_photos(
        chat_id=tg_id,
        text=text_7_for_pro,
        photos=photos,
        post_id="7pro",
    )

    await state.set_state(StoryState.pro_path)

    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"pro_text_8:{callback.from_user.id}",
        run_date=run_date,
        func=send_pro_text_8,
        args=[callback.message.chat.id],
    )

    await callback.answer()
