import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
)
from sqlalchemy import select

from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, scheduler
from routers.novice import send_novice_text_2, send_novice_text_3
from routers.novice_continued import (
    send_novice_text_4,
    send_novice_text_5,
    send_novice_text_6,
    send_novice_text_7,
    send_reviews_auto,
)
from routers.pro import send_pro_text_8, send_pro_text_9
from routers.pro_continued import (
    send_pro_reviews_auto,
    send_pro_text_10,
    send_pro_text_11,
    send_pro_text_12,
)
from utils.scheduler import clear_user_story_jobs

logger = logging.getLogger(__name__)

RECOVERY_NOTICE_TEXT = (
    "Сейчас из-за замедления Telegram тексты могут приходить с задержкой. "
    "Но мы стараемся, чтобы рано или поздно они дошли. Спасибо, что вы с нами 💜"
)


@dataclass(frozen=True)
class RecoveryTransition:
    previous_event: str
    next_events: tuple[str, ...]
    sender: Callable[[int], Awaitable[None]]
    description: str
    segment: str | None = None


RECOVERY_TRANSITIONS: tuple[RecoveryTransition, ...] = (
    RecoveryTransition(
        previous_event="post_sent_1beg",
        next_events=("post_sent_2beg",),
        sender=send_novice_text_2,
        description="beginner: 1 -> 2",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_2beg",
        next_events=("post_sent_3beg",),
        sender=send_novice_text_3,
        description="beginner: 2 -> 3",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_7pro",
        next_events=("post_sent_8pro",),
        sender=send_pro_text_8,
        description="pro: 7 -> 8",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="post_sent_8pro",
        next_events=("post_sent_9pro",),
        sender=send_pro_text_9,
        description="pro: 8 -> 9",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="survey_yes",
        next_events=("post_sent_4beg",),
        sender=send_novice_text_4,
        description="beginner continued: survey_yes -> 4",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="decided_continue",
        next_events=("post_sent_4beg",),
        sender=send_novice_text_4,
        description="beginner continued: decided_continue -> 4",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="survey_yes",
        next_events=("post_sent_10pro",),
        sender=send_pro_text_10,
        description="pro continued: survey_yes -> 10",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="decided_continue",
        next_events=("post_sent_10pro",),
        sender=send_pro_text_10,
        description="pro continued: decided_continue -> 10",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="post_sent_4beg",
        next_events=("post_sent_5beg",),
        sender=send_novice_text_5,
        description="beginner continued: 4 -> 5",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_5beg",
        next_events=("post_sent_6beg",),
        sender=send_novice_text_6,
        description="beginner continued: 5 -> 6",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_6beg",
        next_events=("post_sent_8beg",),
        sender=send_novice_text_7,
        description="beginner continued: 6 -> 7/8",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_8beg",
        next_events=("post_sent_final_up",),
        sender=send_reviews_auto,
        description="beginner final: 8 -> final",
        segment="beginner",
    ),
    RecoveryTransition(
        previous_event="post_sent_10pro",
        next_events=("post_sent_11pro",),
        sender=send_pro_text_11,
        description="pro continued: 10 -> 11",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="post_sent_11pro",
        next_events=("post_sent_12pro",),
        sender=send_pro_text_12,
        description="pro continued: 11 -> 12",
        segment="pro",
    ),
    RecoveryTransition(
        previous_event="post_sent_12pro",
        next_events=("post_sent_final_up",),
        sender=send_pro_reviews_auto,
        description="pro final: 12 -> final",
        segment="pro",
    ),
)

RELEVANT_EVENT_NAMES = {
    event_name
    for transition in RECOVERY_TRANSITIONS
    for event_name in (transition.previous_event, *transition.next_events)
}


@dataclass
class RecoveryCandidate:
    tg_id: int
    segment: str | None
    transition: RecoveryTransition
    previous_timestamp: datetime


def latest_timestamp(
    events: list[tuple[str, datetime]], event_names: tuple[str, ...]
) -> datetime | None:
    latest: datetime | None = None
    event_name_set = set(event_names)
    for event_name, timestamp in events:
        if event_name not in event_name_set:
            continue
        if latest is None or timestamp > latest:
            latest = timestamp
    return latest


async def load_user_events() -> dict[int, dict[str, object]]:
    async with db_helper.session_factory() as session:
        users_result = await session.execute(select(User.id, User.tg_id, User.segment))
        users = users_result.all()

        events_result = await session.execute(
            select(Events.user_id, Events.event_name, Events.timestamp)
            .where(Events.event_name.in_(RELEVANT_EVENT_NAMES))
            .order_by(Events.user_id, Events.timestamp)
        )
        rows = events_result.all()

    data: dict[int, dict[str, object]] = {
        user_id: {"tg_id": tg_id, "segment": segment, "events": []}
        for user_id, tg_id, segment in users
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

        tg_id = user_data["tg_id"]
        segment = user_data["segment"]
        best_candidate: RecoveryCandidate | None = None

        for transition in RECOVERY_TRANSITIONS:
            if transition.segment and transition.segment != segment:
                continue

            previous_timestamp = latest_timestamp(events, (transition.previous_event,))
            if previous_timestamp is None:
                continue

            next_timestamp = latest_timestamp(events, transition.next_events)
            if next_timestamp is not None and next_timestamp > previous_timestamp:
                continue

            if now - previous_timestamp < stale_after:
                continue

            candidate = RecoveryCandidate(
                tg_id=tg_id,
                segment=segment,
                transition=transition,
                previous_timestamp=previous_timestamp,
            )
            if (
                best_candidate is None
                or candidate.previous_timestamp > best_candidate.previous_timestamp
            ):
                best_candidate = candidate

        if best_candidate is not None:
            candidates.append(best_candidate)

    candidates.sort(key=lambda item: item.previous_timestamp)
    return candidates


async def send_recovery_notice(chat_id: int) -> None:
    await bot.send_message(chat_id=chat_id, text=RECOVERY_NOTICE_TEXT)


async def recover_candidates(
    candidates: list[RecoveryCandidate],
    *,
    dry_run: bool,
) -> tuple[int, int]:
    restored = 0
    failed = 0

    for candidate in candidates:
        age_hours = (
            datetime.now(timezone.utc) - candidate.previous_timestamp
        ).total_seconds() / 3600
        logger.info(
            "Candidate tg_id=%s segment=%s transition=%s age_hours=%.1f",
            candidate.tg_id,
            candidate.segment,
            candidate.transition.description,
            age_hours,
        )

        if dry_run:
            continue

        try:
            clear_user_story_jobs(tg_id=candidate.tg_id)
            await candidate.transition.sender(candidate.tg_id)
            await send_recovery_notice(candidate.tg_id)
            restored += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramAPIError) as exc:
            failed += 1
            logger.warning(
                "Failed to recover chain for tg_id=%s: %s",
                candidate.tg_id,
                exc,
            )
        except Exception:
            failed += 1
            logger.exception("Unexpected recovery error for tg_id=%s", candidate.tg_id)

    return restored, failed


async def async_main(hours: int, dry_run: bool) -> None:
    stale_after = timedelta(hours=hours)
    scheduler.start(paused=True)
    try:
        users_data = await load_user_events()
        candidates = build_candidates(users_data, stale_after=stale_after)
        logger.info("Found %s recovery candidates", len(candidates))
        restored, failed = await recover_candidates(candidates, dry_run=dry_run)
        logger.info(
            "Recovery finished. dry_run=%s restored=%s failed=%s",
            dry_run,
            restored,
            failed,
        )
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await bot.session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore interrupted bot chains from event history."
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Minimal age of the last sent step before recovery.",
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
