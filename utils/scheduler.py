from collections.abc import Callable
from datetime import datetime
from data.states import StoryState
from aiogram import Bot
from aiogram.utils.keyboard import InlineKeyboardBuilder
from loader import bot, dp, scheduler

from data.story_content import text_after_15_minutes


def schedule_user_job(
    *,
    job_id: str,
    run_date: datetime,
    func: Callable,
    args: list,
) -> None:
    if run_date.tzinfo is None:
        run_date = run_date.astimezone(scheduler.timezone)

    scheduler.add_job(
        func,
        trigger="date",
        run_date=run_date,
        args=args,
        id=job_id,
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
        max_instances=1,
    )


async def send_15min_survey(chat_id: int):
    state = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    current_state = await state.get_state()

    if current_state != StoryState.waiting_15min_pause.state:
        return

    await state.set_state(StoryState.waiting_for_extra_materials)

    builder = InlineKeyboardBuilder()
    builder.button(text="Да", callback_data="extra_yes")
    builder.button(text="Нет", callback_data="extra_no")
    builder.adjust(2)

    await bot.send_message(
        chat_id, text_after_15_minutes, reply_markup=builder.as_markup()
    )
