import argparse
import asyncio
import contextlib
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import exists, select

from config import settings
from db.crud import add_event
from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, logger, redis

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DEFAULT_RUN_AT = datetime(2026, 7, 2, 12, 6, tzinfo=MOSCOW_TZ)
BROADCAST_SENT_EVENT = "mini_guide_day_8_sent_2026_07_02_1206"
SECOND_MESSAGE_DELAY_SECONDS = 2 * 60
USER_START_DELAY_SECONDS = 0.1

FIRST_MESSAGE = """8️⃣ <b>Как говорить о направлении</b>
Сохраняйте позицию союзника: направление — это расширение заботы, а не «передача проблемы».

<b>Примеры фраз:</b>
🔹 «Часть того, что вы описываете, может быть связана с рисками для физического здоровья. Чтобы мы работали безопасно, я рекомендую консультацию врача/психиатра. Я могу помочь организовать этот шаг».

🔹 «Психотерапия важна, и параллельно стоит оценить состояние организма. Это стандартная практика при похожих симптомах, а не что-то "не так" с вами»."""

SECOND_MESSAGE = """Что ж. Наш мини-гайд “Как говорить с клиентом о еде и весе, чтобы помочь, а не навредить” закончился. Мы старались быть настолько краткими насколько возможно 😁

Поделитесь пожалуйста, понравилось ли вам?"""


def get_feedback_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да",
                    callback_data="mini_guide_day8_feedback:yes",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Нет",
                    callback_data="mini_guide_day8_feedback:no",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Можно сложнее, мы готовы 😁",
                    callback_data="mini_guide_day8_feedback:harder",
                )
            ],
        ]
    )


async def load_recipients(*, include_already_sent: bool) -> list[int]:
    async with db_helper.session_factory() as session:
        sent_exists = (
            exists()
            .where(
                Events.user_id == User.id,
                Events.event_name == BROADCAST_SENT_EVENT,
            )
            .correlate(User)
        )
        stmt = select(User.tg_id).order_by(User.id)
        if not include_already_sent:
            stmt = stmt.where(~sent_exists)

        result = await session.execute(stmt)
        return list(result.scalars().all())


async def wait_until(run_at: datetime) -> bool:
    now = datetime.now(MOSCOW_TZ)
    if run_at <= now:
        logger.warning(
            "Scheduled time %s Moscow time is already in the past. "
            "Use --now to send immediately.",
            run_at.isoformat(),
        )
        return False

    delay_seconds = (run_at - now).total_seconds()
    logger.info("Waiting until %s Moscow time.", run_at.isoformat())
    await asyncio.sleep(delay_seconds)
    return True


async def send_message_safely(
    user_id: int,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
        return True
    except TelegramRetryAfter as exc:
        logger.warning("Rate limit for user %s, sleeping %s seconds", user_id, exc.retry_after)
        await asyncio.sleep(exc.retry_after)
        return await send_message_safely(user_id, text, reply_markup=reply_markup)
    except TelegramForbiddenError:
        logger.warning("User %s blocked the bot.", user_id)
        return False
    except TelegramAPIError:
        logger.exception("Telegram error while sending to %s", user_id)
        return False
    except Exception:
        logger.exception("Failed to send message to user %s", user_id)
        return False


async def send_user_chain(user_id: int, *, mark_sent_event: bool) -> tuple[bool, bool]:
    first_sent = await send_message_safely(user_id, FIRST_MESSAGE)
    if not first_sent:
        return False, False

    await asyncio.sleep(SECOND_MESSAGE_DELAY_SECONDS)
    second_sent = await send_message_safely(
        user_id,
        SECOND_MESSAGE,
        reply_markup=get_feedback_keyboard(),
    )
    if second_sent and mark_sent_event:
        await add_event(tg_id=user_id, event_name=BROADCAST_SENT_EVENT)

    return True, second_sent


async def send_test_admin() -> None:
    first_sent, second_sent = await send_user_chain(
        settings.ADMIN_ID,
        mark_sent_event=False,
    )
    logger.info(
        "Admin test finished for ADMIN_ID=%s: first=%s, second=%s",
        settings.ADMIN_ID,
        first_sent,
        second_sent,
    )


async def send_broadcast(user_ids: list[int], *, dry_run: bool) -> None:
    if dry_run:
        logger.info("Dry run recipients count: %s", len(user_ids))
        logger.info("Dry run recipients: %s", user_ids)
        return

    tasks = []
    for index, user_id in enumerate(user_ids, start=1):
        tasks.append(asyncio.create_task(send_user_chain(user_id, mark_sent_event=True)))
        logger.info("Scheduled user %s/%s: %s", index, len(user_ids), user_id)
        await asyncio.sleep(USER_START_DELAY_SECONDS)

    results = await asyncio.gather(*tasks)
    first_sent_count = sum(1 for first_sent, _ in results if first_sent)
    second_sent_count = sum(1 for _, second_sent in results if second_sent)
    logger.info(
        "Mini-guide day 8 stats: first=%s/%s, second=%s/%s",
        first_sent_count,
        len(user_ids),
        second_sent_count,
        len(user_ids),
    )


async def run_broadcast(*, dry_run: bool, include_already_sent: bool) -> None:
    user_ids = await load_recipients(include_already_sent=include_already_sent)
    if not user_ids:
        logger.warning("No recipients found")
        return

    logger.info("Starting mini-guide day 8 broadcast for %s users", len(user_ids))
    await send_broadcast(user_ids, dry_run=dry_run)
    logger.info("Mini-guide day 8 broadcast finished")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send mini-guide day 8 broadcast chain to RPP bot users."
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Send immediately instead of waiting until 2026-07-02 12:06 MSK.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print recipient count and ids, do not send messages.",
    )
    parser.add_argument(
        "--include-already-sent",
        action="store_true",
        help="Send even to users who already have this broadcast sent event.",
    )
    parser.add_argument(
        "--test-admin",
        action="store_true",
        help="Send the two-message chain only to ADMIN_ID.",
    )
    args = parser.parse_args()

    try:
        if args.test_admin:
            await send_test_admin()
            return

        if not args.now and not args.dry_run:
            should_send = await wait_until(DEFAULT_RUN_AT)
            if not should_send:
                return

        await run_broadcast(
            dry_run=args.dry_run,
            include_already_sent=args.include_already_sent,
        )
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()
        with contextlib.suppress(Exception):
            if redis is not None:
                await redis.aclose()
        with contextlib.suppress(Exception):
            await db_helper.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
