import argparse
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import exists, select

from config import settings
from db.crud import add_event
from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, redis
from routers.start import get_subscription_channels

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DEFAULT_RUN_AT = datetime(2026, 5, 1, 12, 0, tzinfo=MOSCOW_TZ)
SOURCE_EVENT = "extra_yes"
BROADCAST_SENT_EVENT = "extra_yes_subscription_broadcast_sent"
BROADCAST_TEXT = """
Коллеги, мы перевели и оформили <b>пенсильванский опросник беспокойства</b>.

PSWQ (Penn State Worry Questionnaire) — один из самых надёжных и валидизированных инструментов оценки тревожного беспокойства. Признан «золотым стандартом» в клинической и исследовательской практике.

<b>Что измеряет</b>: 
🔹 избыточность и навязчивость беспокойства 
🔹 степень утраты контроля над тревожными мыслями 
🔹 подходит для диагностики и мониторинга динамики в терапии

Результат не зависит от настроения клиента в день заполнения — опросник измеряет не текущую тревогу, а то насколько человеку в целом свойственно беспокоиться. Это делает его универсальным инструментом вне зависимости от вашего терапевтического подхода.

🎁 <b>Скорее всего Вы уже подписаны на Школу ЧК, а теперь предлагаем подписаться на канал Ирины Ушковой, чтобы получить файл.</b>
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def build_keyboard():
    builder = InlineKeyboardBuilder()
    for index, (_, url) in enumerate(get_subscription_channels(), start=1):
        builder.button(text=f"{index}. Подписаться", url=url)
    builder.button(
        text=f"{len(get_subscription_channels()) + 1}. Я подписался!",
        callback_data="check_extra_yes_sub",
    )
    builder.adjust(1)
    return builder.as_markup()


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
        stmt = (
            select(User.tg_id)
            .join(Events, Events.user_id == User.id)
            .where(Events.event_name == SOURCE_EVENT)
            .distinct()
            .order_by(User.tg_id)
        )
        if not include_already_sent:
            stmt = stmt.where(~sent_exists)

        result = await session.execute(stmt)
        return list(result.scalars())


async def wait_until(run_at: datetime) -> None:
    now = datetime.now(MOSCOW_TZ)
    if run_at <= now:
        logger.info("Run time is in the past, sending immediately.")
        return

    delay_seconds = (run_at - now).total_seconds()
    logger.info("Waiting until %s Moscow time.", run_at.isoformat())
    await asyncio.sleep(delay_seconds)


async def send_broadcast(*, dry_run: bool, include_already_sent: bool) -> None:
    recipients = await load_recipients(include_already_sent=include_already_sent)
    logger.info("Recipients found: %s", len(recipients))

    if dry_run:
        logger.info("Dry run recipients: %s", recipients)
        return

    keyboard = build_keyboard()
    sent = 0
    failed = 0

    for tg_id in recipients:
        try:
            await bot.send_message(
                chat_id=tg_id,
                text=BROADCAST_TEXT,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await add_event(tg_id=tg_id, event_name=BROADCAST_SENT_EVENT)
            sent += 1
            logger.info("Sent to %s", tg_id)
            await asyncio.sleep(0.05)
        except TelegramForbiddenError:
            failed += 1
            logger.warning("User %s blocked the bot.", tg_id)
        except TelegramAPIError:
            failed += 1
            logger.exception("Telegram error while sending to %s", tg_id)

    logger.info("Done. Sent: %s. Failed: %s.", sent, failed)


async def send_test_message() -> None:
    await bot.send_message(
        chat_id=settings.ADMIN_ID,
        text=BROADCAST_TEXT,
        reply_markup=build_keyboard(),
        parse_mode="HTML",
    )
    logger.info("Test message sent to ADMIN_ID=%s.", settings.ADMIN_ID)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send scheduled subscription broadcast to users with extra_yes."
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Send immediately instead of waiting until 2026-05-01 12:00 MSK.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print recipient count and ids, do not send messages.",
    )
    parser.add_argument(
        "--include-already-sent",
        action="store_true",
        help="Send even to users who already have broadcast sent event.",
    )
    parser.add_argument(
        "--test-admin",
        action="store_true",
        help="Send the broadcast message only to ADMIN_ID immediately.",
    )
    args = parser.parse_args()

    if args.test_admin:
        try:
            await send_test_message()
        finally:
            await bot.session.close()
            await db_helper.engine.dispose()
            if redis is not None:
                await redis.aclose()
        return

    if not args.now and not args.dry_run:
        await wait_until(DEFAULT_RUN_AT)

    try:
        await send_broadcast(
            dry_run=args.dry_run,
            include_already_sent=args.include_already_sent,
        )
    finally:
        await bot.session.close()
        await db_helper.engine.dispose()
        if redis is not None:
            await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
