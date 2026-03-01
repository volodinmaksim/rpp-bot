from contextlib import suppress

from aiogram import Router, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from data.states import StoryState
from data.story_content import text_extra_no
from db.crud import add_event
from loader import bot

router = Router()


@router.callback_query(F.data == "extra_no", StoryState.waiting_for_extra_materials)
async def process_decline(callback: types.CallbackQuery):
    builder = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Я передумал, хочу материалы! ⚡️",
                    callback_data="extra_yes",
                )
            ]
        ]
    )
    with suppress(TelegramBadRequest):
        await callback.message.delete()
    photo_path = FSInputFile("data/photos/meme2.png")
    await bot.send_photo(
        chat_id=callback.from_user.id,
        photo=photo_path,
        caption=text_extra_no,
        parse_mode="HTML",
        reply_markup=builder,
    )

    await callback.answer()


@router.callback_query(F.data == "extra_yes")
async def process_accept(callback: types.CallbackQuery, state: FSMContext):

    await state.set_state(StoryState.choosing_experience)

    text = (
        "Круто, что вы с нами дальше! ✨\n\n"
        "Чтобы мы могли прислать что-то релевантное вашим "
        "интересам, поделитесь пожалуйста какой у вас опыт."
    )

    builder = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Я начинающий в теме РПП", callback_data="exp_beginner"
                )
            ],
            [
                types.InlineKeyboardButton(
                    text="Я работаю с темой РПП", callback_data="exp_pro"
                )
            ],
        ]
    )

    with suppress(TelegramBadRequest):
        await callback.message.delete()
    await callback.message.answer(text=text, reply_markup=builder)
    await callback.answer()
