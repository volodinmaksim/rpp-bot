import asyncio
from datetime import datetime, timedelta

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext

from data.states import StoryState
from data.story_content import (
    text_1_for_beginners,
    text_2_for_beginners,
    text_3_for_beginners,
    survey_question_text,
    text_after_level,
)
from db.crud import add_event, set_segment
from loader import bot, dp
from utils.common import get_next_working_time, my_send_photos
from utils.keyboards import get_feedback_kb, get_survey_kb
from utils.scheduler import schedule_user_job

router = Router()


async def send_novice_text_3(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.novice_path)

    photos = [f"data/photos/text_3_beginers_{i}.jpg" for i in range(1, 8)]
    await my_send_photos(
        chat_id=chat_id,
        text=text_3_for_beginners,
        photos=photos,
        post_id="3beg",
    )

    await send_survey_after_novice(chat_id)


async def send_novice_text_2(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.novice_path)

    photos = [f"data/photos/text_2_beginers_{i}.jpg" for i in range(1, 10)]
    await my_send_photos(
        chat_id=chat_id,
        text=text_2_for_beginners,
        photos=photos,
        post_id="2beg",
    )


    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"novice_text_3:{chat_id}",
        run_date=run_date,
        func=send_novice_text_3,
        args=[chat_id],
    )


async def send_survey_after_novice(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.waiting_for_survey_response)

    await bot.send_message(
        chat_id=chat_id,
        text=survey_question_text,
        reply_markup=get_survey_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "exp_beginner", StoryState.choosing_experience)
async def start_novice_path(callback: types.CallbackQuery, state: FSMContext):

    await set_segment(tg_id=callback.from_user.id, segment="beginner")

    await callback.message.edit_text(text=text_after_level)
    await asyncio.sleep(2)
    await callback.message.answer(
        text=text_1_for_beginners,
        reply_markup=get_feedback_kb(post_id="1beg"),
        parse_mode="HTML",
    )

    await state.set_state(StoryState.novice_path)

    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"novice_text_2:{callback.from_user.id}",
        run_date=run_date,
        func=send_novice_text_2,
        args=[callback.message.chat.id],
    )

    await callback.answer()


@router.callback_query(F.data.startswith("fb_"))
async def handle_feedback(callback: types.CallbackQuery):
    data = callback.data.split("_")
    vote_type = data[1]
    post_id = data[2]
    await add_event(
        tg_id=callback.from_user.id,
        event_name=f"feedback_{vote_type}_{post_id}",
    )

    await callback.answer("Спасибо за обратную связь! ❤️")
    await callback.message.edit_reply_markup(reply_markup=None)
