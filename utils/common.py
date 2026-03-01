from datetime import datetime, timedelta

from aiogram.types import InputMediaPhoto, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

from loader import bot
from utils.keyboards import get_feedback_kb


def get_next_working_time():
    run_date = datetime.now() + timedelta(days=1)
    if run_date.hour < 9:
        return run_date.replace(hour=10, minute=0)
    if run_date.hour >= 21:
        return (run_date + timedelta(days=1)).replace(hour=10, minute=0)
    return run_date


async def my_send_video(chat_id: int, text: str, path: str, post_id: str | None = None):
    video = FSInputFile(path)
    await bot.send_video(
        chat_id=chat_id,
        video=video,
        caption=text,
        parse_mode="HTML",
        supports_streaming=True,
    )
    if post_id:
        await bot.send_message(
            chat_id,
            "Как вам материал?",
            reply_markup=get_feedback_kb(post_id=post_id),
        )


async def my_send_photos(
    chat_id: int,
    text: str,
    photos: list[str],
    post_id: str | None = None,
):
    media = []
    for i, path in enumerate(photos):
        if i == 0:
            media.append(
                InputMediaPhoto(
                    media=FSInputFile(path),
                    caption=text,
                    parse_mode="HTML",
                )
            )
        else:
            media.append(InputMediaPhoto(media=FSInputFile(path)))

    await bot.send_media_group(chat_id=chat_id, media=media)
    if post_id:
        await bot.send_message(
            chat_id=chat_id,
            text="Как вам материал?",
            reply_markup=get_feedback_kb(post_id=post_id),
        )


async def my_send_text_and_photos(
    chat_id: int,
    text: str,
    photos: list[str],
    post_id: str,
):
    if len(photos) == 1:
        await bot.send_photo(
            chat_id=chat_id,
            photo=FSInputFile(photos[0]),
            parse_mode="HTML",
        )
    else:
        media = [InputMediaPhoto(media=FSInputFile(p)) for p in photos]
        await bot.send_media_group(chat_id=chat_id, media=media)

    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=get_feedback_kb(post_id=post_id),
    )
