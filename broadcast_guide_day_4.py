import argparse
import asyncio
import contextlib
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter
from sqlalchemy import exists, select

from config import settings
from db.crud import add_event
from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, logger, redis

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DEFAULT_RUN_AT = datetime(2026, 6, 28, 12, 27, tzinfo=MOSCOW_TZ)
BROADCAST_SENT_EVENT = "mini_guide_day_4_sent_2026_06_28_1227"
USER_SEND_DELAY_SECONDS = 0.05

BROADCAST_TEXT = """4️⃣ <b>Компенсаторное поведение (зона повышенного риска)</b>
Компенсаторные стратегии часто скрываются из-за стыда, но значительно повышают медицинские риски.

<b>Спросите:</b>
🔹 «Если вы съедаете больше, чем планировали, возникает ли желание “отработать” это? Например, специально голодать, чрезмерно тренироваться или избавляться от съеденного?»
🔹 «Бывает ли, что вы вызываете рвоту или принимаете слабительные, мочегонные, препараты для снижения веса?»
🔹 «Как часто это происходит?»

Особенно настораживают: регулярная рвота, частые слабительные/диуретики, интенсивные тренировки как «наказание», длительные периоды голодания после переедания."""


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


async def send_message_safely(user_id: int) -> bool:
    try:
        await bot.send_message(
            chat_id=user_id,
            text=BROADCAST_TEXT,
            parse_mode="HTML",
        )
        return True
    except TelegramRetryAfter as exc:
        logger.warning("Rate limit for user %s, sleeping %s seconds", user_id, exc.retry_after)
        await asyncio.sleep(exc.retry_after)
        return await send_message_safely(user_id)
    except TelegramForbiddenError:
        logger.warning("User %s blocked the bot.", user_id)
        return False
    except TelegramAPIError:
        logger.exception("Telegram error while sending to %s", user_id)
        return False
    except Exception:
        logger.exception("Failed to send message to user %s", user_id)
        return False


async def send_test_admin() -> None:
    sent = await send_message_safely(settings.ADMIN_ID)
    logger.info("Admin test finished for ADMIN_ID=%s: sent=%s", settings.ADMIN_ID, sent)


async def send_broadcast(*, dry_run: bool, include_already_sent: bool) -> None:
    recipients = await load_recipients(include_already_sent=include_already_sent)
    logger.info("Recipients found: %s", len(recipients))

    if dry_run:
        logger.info("Dry run recipients: %s", recipients)
        return

    sent = 0
    failed = 0

    for index, tg_id in enumerate(recipients, start=1):
        if await send_message_safely(tg_id):
            await add_event(tg_id=tg_id, event_name=BROADCAST_SENT_EVENT)
            sent += 1
            logger.info("Sent to %s (%s/%s)", tg_id, index, len(recipients))
        else:
            failed += 1

        await asyncio.sleep(USER_SEND_DELAY_SECONDS)

    logger.info("Done. Sent: %s. Failed: %s.", sent, failed)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send mini-guide day 4 broadcast to all RPP bot users."
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Send immediately instead of waiting until 2026-06-28 12:27 MSK.",
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
        help="Send the broadcast message only to ADMIN_ID immediately.",
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

        await send_broadcast(
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
