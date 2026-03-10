from collections.abc import Callable
from datetime import datetime
from data.states import StoryState
from aiogram import Bot
from apscheduler.jobstores.base import JobLookupError
from aiogram.utils.keyboard import InlineKeyboardBuilder

from db.crud import add_event
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


def clear_user_story_jobs(*, tg_id: int) -> None:
    """Удаляет отложенные задачи пользователя, чтобы цепочки не пересекались."""
    job_prefixes = (
        "15min_survey",
        "novice_text_2",
        "novice_text_3",
        "novice_text_4",
        "novice_text_5",
        "novice_text_6",
        "novice_text_7",
        "novice_text_8",
        "pro_text_8",
        "pro_text_9",
        "pro_text_10",
        "pro_text_11",
        "pro_text_12",
        "pro_reviews",
        "continued_path",
    )

    for prefix in job_prefixes:
        try:
            scheduler.remove_job(job_id=f"{prefix}:{tg_id}")
        except JobLookupError:
            continue


async def send_15min_survey(chat_id: int):
    state = dp.fsm.resolve_context(bot=bot, chat_id=chat_id, user_id=chat_id)
    current_state = await state.get_state()

    if current_state != StoryState.waiting_15min_pause.state:
        return

    await add_event(
        tg_id=chat_id,
        event_name="survey_15min_sent",
    )

    await state.set_state(StoryState.waiting_for_extra_materials)

    builder = InlineKeyboardBuilder()
    builder.button(text="Да", callback_data="extra_yes")
    builder.button(text="Нет", callback_data="extra_no")
    builder.adjust(2)

    await bot.send_message(
        chat_id, text_after_15_minutes, reply_markup=builder.as_markup()
    )
