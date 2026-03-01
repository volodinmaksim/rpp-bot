import asyncio
from datetime import datetime, timedelta
from aiogram import Router
from aiogram.types import FSInputFile, InputMediaPhoto

from data.states import StoryState
from data.story_content import (
    text_4_for_beginners,
    text_5_for_beginners,
    text_6_for_beginners,
    text_7_for_beginners,
    text_8_for_beginners,
    final_goodbye_text_up,
)
from loader import dp, bot
from utils.common import my_send_photos, my_send_video, get_next_working_time
from utils.keyboards import get_feedback_kb, get_reviews_kb
from utils.scheduler import schedule_user_job



router = Router()

def calculate_run_date():
    """Считает время: сейчас + 3 часа, но строго в интервале 10:00 - 21:00"""
    run_date = datetime.now() + timedelta(hours=3)

    if run_date.hour >= 21:
        run_date = (run_date + timedelta(days=1)).replace(hour=10, minute=0, second=0)

    elif run_date.hour < 10:
        run_date = run_date.replace(hour=10, minute=0, second=0)

    return run_date


async def send_reviews_auto(chat_id: int):
    photo = FSInputFile("data/photos/text_final.jpg")
    await bot.send_photo(
        chat_id=chat_id,
        photo=photo,
        caption=final_goodbye_text_up,
        parse_mode="HTML",
    )


async def send_novice_text_7(chat_id: int):
    state_context = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    await state_context.set_state(StoryState.final_stage)

    await bot.send_message(chat_id, text_7_for_beginners, parse_mode="HTML")
    await asyncio.sleep(6)

    photos = [f"data/photos/text_8_beginers_{i}.jpg" for i in range(1, 7)]
    media = [
        InputMediaPhoto(media=FSInputFile(p), parse_mode="HTML" if i == 0 else None)
        for i, p in enumerate(photos)
    ]

    await bot.send_media_group(chat_id=chat_id, media=media)
    await bot.send_message(
        chat_id=chat_id,
        text=text_8_for_beginners,
        parse_mode="HTML",
        reply_markup=get_reviews_kb(),
    )
    run_date = calculate_run_date()
    schedule_user_job(
        job_id=f"novice_reviews:{chat_id}",
        run_date=run_date,
        func=send_reviews_auto,
        args=[chat_id],
    )


async def send_novice_text_6(chat_id: int):
    await bot.send_message(
        chat_id,
        text_6_for_beginners,
        reply_markup=get_feedback_kb(post_id="6"),
        parse_mode="HTML",
    )
    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"novice_text_7:{chat_id}",
        run_date=run_date,
        func=send_novice_text_7,
        args=[chat_id],
    )

async def send_novice_text_5(chat_id: int):
    photos = [f"data/photos/text_5_beginers_{i}.jpg" for i in range(1, 7)]
    await my_send_photos(
        chat_id=chat_id, text=text_5_for_beginners, photos=photos, post_id="5"
    )

    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"novice_text_6:{chat_id}",
        run_date=run_date,
        func=send_novice_text_6,
        args=[chat_id],
    )


async def send_novice_text_4(chat_id: int):
    path = "data/videos/text_4_beginer_1.MOV"
    await my_send_video(
        chat_id=chat_id, text=text_4_for_beginners, path=path, post_id="4"
    )
    run_date = get_next_working_time()
    schedule_user_job(
        job_id=f"novice_text_5:{chat_id}",
        run_date=run_date,
        func=send_novice_text_5,
        args=[chat_id],
    )
