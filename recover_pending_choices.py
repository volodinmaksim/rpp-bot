import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from aiogram import types
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
)
from aiogram.fsm.state import State
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select

from config import settings
from data.states import StoryState
from data.story_content import (
    final_goodbye_text_down,
    survey_question_text,
    text_after_15_minutes,
    text_hello,
)
from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, dp, redis
from utils.keyboards import get_survey_kb
from utils.scheduler import clear_user_story_jobs

logger = logging.getLogger(__name__)

EXPERIENCE_CHOICE_TEXT = (
    "Круто, что вы с нами дальше! ?\n\n"
    "Чтобы мы могли прислать что-то релевантное вашим "
    "интересам, поделитесь пожалуйста какой у вас опыт."
)

RECOVERY_PREFIX = (
    "Привет. Из-за замедления Telegram ответ на кнопку мог не обработаться с первого раза.\n\n"
    "Если вы уже пытались ответить, пожалуйста, выберите еще раз. Если еще не пытались, извините за беспокойство."
)


@dataclass(frozen=True)
class PendingChoiceRecovery:
    waiting_events: tuple[str, ...]
    completion_events: tuple[str, ...]
    state: State | None
    sender: Callable[[int], Awaitable[None]]
    description: str


@dataclass
class RecoveryCandidate:
    tg_id: int
    recovery: PendingChoiceRecovery
    waiting_timestamp: datetime


async def send_recovery_prefix(chat_id: int) -> None:
    await bot.send_message(chat_id=chat_id, text=RECOVERY_PREFIX)


async def send_subscription_choice(chat_id: int) -> None:
    builder = InlineKeyboardBuilder()
    channels = (
        settings.CHAT_URL,
        settings.SECOND_CHAT_URL,
    )
    for index, url in enumerate(channels, start=1):
        builder.button(text=f"{index}. Подписаться", url=url)
    builder.button(text=f"{len(channels) + 1}. Я подписался!", callback_data="check_sub")
    builder.adjust(1)

    await send_recovery_prefix(chat_id)
    await bot.send_message(
        chat_id=chat_id,
        text=text_hello,
        reply_markup=builder.as_markup(),
    )


async def send_extra_materials_choice(chat_id: int) -> None:
    builder = InlineKeyboardBuilder()
    builder.button(text="Да", callback_data="extra_yes")
    builder.button(text="Нет", callback_data="extra_no")
    builder.adjust(2)

    await send_recovery_prefix(chat_id)
    await bot.send_message(
        chat_id=chat_id,
        text=text_after_15_minutes,
        reply_markup=builder.as_markup(),
    )


async def send_experience_choice(chat_id: int) -> None:
    builder = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="Я начинающий в теме РПП",
                    callback_data="exp_beginner",
                )
            ],
            [
                types.InlineKeyboardButton(
                    text="Я работаю с темой РПП",
                    callback_data="exp_pro",
                )
            ],
        ]
    )

    await send_recovery_prefix(chat_id)
    await bot.send_message(
        chat_id=chat_id,
        text=EXPERIENCE_CHOICE_TEXT,
        reply_markup=builder,
    )


async def send_survey_choice(chat_id: int) -> None:
    await send_recovery_prefix(chat_id)
    await bot.send_message(
        chat_id=chat_id,
        text=survey_question_text,
        reply_markup=get_survey_kb(),
        parse_mode="HTML",
    )


async def send_continue_choice(chat_id: int) -> None:
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

    await send_recovery_prefix(chat_id)
    photo = FSInputFile("data/photos/text_final.jpg")
    await bot.send_photo(
        chat_id=chat_id,
        photo=photo,
        caption=final_goodbye_text_down,
        parse_mode="HTML",
        reply_markup=builder,
    )


RECOVERY_RULES: tuple[PendingChoiceRecovery, ...] = (
    PendingChoiceRecovery(
        waiting_events=("user_joined",),
        completion_events=(
            "survey_15min_sent",
            "extra_yes",
            "extra_no",
            "post_sent_1beg",
            "post_sent_7pro",
        ),
        state=StoryState.waiting_for_subscription,
        sender=send_subscription_choice,
        description="subscription confirmation",
    ),
    PendingChoiceRecovery(
        waiting_events=("survey_15min_sent",),
        completion_events=("extra_yes", "extra_no", "post_sent_1beg", "post_sent_7pro"),
        state=StoryState.waiting_for_extra_materials,
        sender=send_extra_materials_choice,
        description="extra materials choice",
    ),
    PendingChoiceRecovery(
        waiting_events=("extra_yes",),
        completion_events=("post_sent_1beg", "post_sent_7pro"),
        state=StoryState.choosing_experience,
        sender=send_experience_choice,
        description="experience choice",
    ),
    PendingChoiceRecovery(
        waiting_events=("post_sent_3beg", "post_sent_9pro"),
        completion_events=(
            "survey_yes",
            "survey_no",
            "decided_continue",
            "post_sent_4beg",
            "post_sent_10pro",
            "post_sent_final_down",
        ),
        state=StoryState.waiting_for_survey_response,
        sender=send_survey_choice,
        description="survey choice",
    ),
    PendingChoiceRecovery(
        waiting_events=("survey_no", "post_sent_final_down"),
        completion_events=(
            "decided_continue",
            "wish_submitted",
            "post_sent_4beg",
            "post_sent_10pro",
        ),
        state=None,
        sender=send_continue_choice,
        description="continue after opt-out",
    ),
)

RELEVANT_EVENT_NAMES = {
    event_name
    for rule in RECOVERY_RULES
    for event_name in (*rule.waiting_events, *rule.completion_events)
}


def latest_timestamp(
    events: list[tuple[str, datetime]], event_names: tuple[str, ...]
) -> datetime | None:
    event_set = set(event_names)
    latest: datetime | None = None
    for event_name, timestamp in events:
        if event_name not in event_set:
            continue
        if latest is None or timestamp > latest:
            latest = timestamp
    return latest


async def load_user_events() -> dict[int, dict[str, object]]:
    async with db_helper.session_factory() as session:
        users_result = await session.execute(
            select(User.id, User.tg_id, User.join_date)
        )
        users = users_result.all()

        events_result = await session.execute(
            select(Events.user_id, Events.event_name, Events.timestamp)
            .where(Events.event_name.in_(RELEVANT_EVENT_NAMES))
            .order_by(Events.user_id, Events.timestamp)
        )
        rows = events_result.all()

    data: dict[int, dict[str, object]] = {
        user_id: {
            "tg_id": tg_id,
            "join_date": join_date,
            "events": [("user_joined", join_date)] if join_date else [],
        }
        for user_id, tg_id, join_date in users
    }
    for user_id, event_name, timestamp in rows:
        if user_id not in data or timestamp is None:
            continue
        data[user_id]["events"].append((event_name, timestamp))
    return data


def build_candidates(
    users_data: dict[int, dict[str, object]],
    stale_after: timedelta,
) -> list[RecoveryCandidate]:
    now = datetime.now(timezone.utc)
    candidates: list[RecoveryCandidate] = []

    for user_data in users_data.values():
        events = user_data["events"]
        if not events:
            continue

        best_candidate: RecoveryCandidate | None = None

        for rule in RECOVERY_RULES:
            waiting_timestamp = latest_timestamp(events, rule.waiting_events)
            if waiting_timestamp is None:
                continue

            completion_timestamp = latest_timestamp(events, rule.completion_events)
            if (
                completion_timestamp is not None
                and completion_timestamp > waiting_timestamp
            ):
                continue

            if now - waiting_timestamp < stale_after:
                continue

            candidate = RecoveryCandidate(
                tg_id=user_data["tg_id"],
                recovery=rule,
                waiting_timestamp=waiting_timestamp,
            )
            if (
                best_candidate is None
                or candidate.waiting_timestamp > best_candidate.waiting_timestamp
            ):
                best_candidate = candidate

        if best_candidate is not None:
            candidates.append(best_candidate)

    candidates.sort(key=lambda item: item.waiting_timestamp)
    return candidates


async def recover_candidates(
    candidates: list[RecoveryCandidate],
    *,
    dry_run: bool,
) -> tuple[int, int]:
    restored = 0
    failed = 0

    for candidate in candidates:
        age_hours = (
            datetime.now(timezone.utc) - candidate.waiting_timestamp
        ).total_seconds() / 3600
        logger.info(
            "Candidate tg_id=%s choice=%s age_hours=%.1f",
            candidate.tg_id,
            candidate.recovery.description,
            age_hours,
        )

        if dry_run:
            continue

        try:
            clear_user_story_jobs(tg_id=candidate.tg_id)
            if candidate.recovery.state is not None:
                state = dp.fsm.resolve_context(
                    bot=bot,
                    chat_id=candidate.tg_id,
                    user_id=candidate.tg_id,
                )
                await state.set_state(candidate.recovery.state)
            await candidate.recovery.sender(candidate.tg_id)
            restored += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramAPIError) as exc:
            failed += 1
            logger.warning(
                "Failed to restore pending choice for tg_id=%s: %s",
                candidate.tg_id,
                exc,
            )
        except Exception:
            failed += 1
            logger.exception(
                "Unexpected pending choice recovery error for tg_id=%s",
                candidate.tg_id,
            )

    return restored, failed


async def async_main(hours: int, dry_run: bool) -> None:
    stale_after = timedelta(hours=hours)
    try:
        users_data = await load_user_events()
        candidates = build_candidates(users_data, stale_after=stale_after)
        logger.info("Found %s pending-choice candidates", len(candidates))
        restored, failed = await recover_candidates(candidates, dry_run=dry_run)
        logger.info(
            "Pending-choice recovery finished. dry_run=%s restored=%s failed=%s",
            dry_run,
            restored,
            failed,
        )
    finally:
        await dp.storage.close()
        if redis is not None:
            await redis.aclose()
        await bot.session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore users who may be stuck on choice buttons."
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Minimal age of waiting state before recovery.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print candidates without sending messages.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s %(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()
    asyncio.run(async_main(hours=args.hours, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
