from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder


def get_feedback_kb(post_id: str):
    builder = InlineKeyboardBuilder()
    # Передаем id поста, чтобы в базе понимать, за какой текст проголосовали
    builder.button(text="👍", callback_data=f"fb_up_{post_id}")
    builder.button(text="👎", callback_data=f"fb_down_{post_id}")
    builder.adjust(2)
    return builder.as_markup()


def get_survey_kb():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="Да", callback_data="survey_yes"),
        types.InlineKeyboardButton(text="Нет", callback_data="survey_no"),
    )
    return builder.as_markup()


def get_reviews_kb():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="А отзывы есть?", callback_data="show_reviews")
    )
    return builder.as_markup()
