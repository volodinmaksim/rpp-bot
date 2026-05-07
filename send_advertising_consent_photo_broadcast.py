import argparse
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.types import FSInputFile
from sqlalchemy import exists, select

from config import settings
from db.crud import add_event
from db.db_helper import db_helper
from db.models import Events, User
from loader import bot, redis

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DEFAULT_RUN_AT = datetime(2026, 5, 8, 12, 0, tzinfo=MOSCOW_TZ)
SOURCE_EVENTS = (
    "click_get_questionnaire",
    'Получить файл: "Пенсильванский опросник"',
)
BROADCAST_SENT_EVENT = "questionnaire_recipients_photo_broadcast_sent"

PHOTO_PATH = Path("data/photos/broadcast_2026_05_08.jpg")
BROADCAST_CAPTION = """
<b>Клиент на ГПП-1: как адаптировать протоколы терапии под «новые тренды»?</b>

В современной практике РПП-специалиста всё чаще звучат названия препаратов-агонистов ГПП-1* (Оземпик и его аналоги). Для многих из нас это становится точкой профессиональной неопределенности.

Важно понимать, что хотя препараты эффективно воздействуют на физиологию, они не затрагивают психологическую архитектуру расстройства. Психотерапия становится критически важным звеном, которое обеспечивает устойчивость изменений.

Препараты ГПП-1 успешно имитируют действие гормонов, повышая сытость и снижая аппетит. Однако РПП — это многофакторное состояние, и работа с ним по-прежнему требует внимания к трем ключевым направлениям:

1. <b>Сверхзначимость формы и веса.</b> Препарат меняет тело, но не отношение к нему. Страх набора веса и когнитивные искажения остаются в зоне ответственности терапевта.
2. <b>Пищевые правила и ритуалы.</b> Лекарство не отменяет мифы о «плохих» продуктах и жесткие запреты, которые продолжают ограничивать жизнь клиента.
3. <b>Эпизоды переедания.</b> Если питание остается нерегулярным, переедания могут сохраняться даже на фоне сниженного аппетита. Психотерапия помогает вернуть структуру и осознанность.

<b>На чем мы фокусируемся в психотерапии РПП?</b>

В работе с клиентами на ГПП-1 мы уделяем особое внимание следующим аспектам:
1. <b>Работа с мифами и страхами:</b> деконструкция ригидных правил и страхов вокруг определенных групп продуктов.
2. <b>Образ тела:</b> работа с проверками (body checking) и избеганием, формирование более бережного и доброжелательного отношения к себе.
3. <b>Поведенческая гибкость:</b> поддержка в восстановлении регулярного и сбалансированного питания, так как со временем организм адаптируется к действию препарата.
4. <b>Долгосрочная перспектива:</b> осознание того, что сама по себе потеря веса не является синонимом ремиссии РПП.

<b>Рост экспертизы в меняющемся мире</b>

Количество клиентских запросов на сочетание медикаментозной поддержки и психотерапии будет только расти. Для специалиста сегодня важно не противопоставлять эти методы, а умело интегрировать их, сохраняя этику и доказательный подход.

Уверенная работа с такими сложными, современными кейсами — это одна из центральных тем, которые мы подробно разбираем на курсе «Психотерапия РПП: продвинутый уровень». 

Мы учим видеть за физиологическими изменениями живого человека и помогаем вам стать тем специалистом, который знает, что делать, когда старые протоколы требуют новых решений.

<b>Узнать больше о программе и записаться на курс:</b>
https://clck.ru/3TTRAZ

<i>*Агонисты рецепторов ГПП-1 — класс препаратов для лечения диабета 2-го типа и ожирения, которые регулируют уровень глюкозы и чувство насыщения.</i>
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


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
            .where(Events.event_name.in_(SOURCE_EVENTS))
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


async def send_photo_to_chat(chat_id: int) -> None:
    await bot.send_photo(
        chat_id=chat_id,
        photo=FSInputFile(PHOTO_PATH),
    )
    await bot.send_message(
        chat_id=chat_id,
        text=BROADCAST_CAPTION,
        parse_mode="HTML",
    )


async def send_test_message() -> None:
    if not PHOTO_PATH.exists():
        raise FileNotFoundError(f"Photo not found: {PHOTO_PATH}")

    await send_photo_to_chat(settings.ADMIN_ID)
    logger.info("Test photo message sent to ADMIN_ID=%s.", settings.ADMIN_ID)


async def send_broadcast(*, dry_run: bool, include_already_sent: bool) -> None:
    recipients = await load_recipients(include_already_sent=include_already_sent)
    logger.info("Recipients found: %s", len(recipients))

    if dry_run:
        logger.info("Dry run recipients: %s", recipients)
        return

    if not PHOTO_PATH.exists():
        raise FileNotFoundError(f"Photo not found: {PHOTO_PATH}")

    sent = 0
    failed = 0

    for tg_id in recipients:
        try:
            await send_photo_to_chat(tg_id)
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


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send scheduled photo broadcast to users who received questionnaire."
    )
    parser.add_argument(
        "--now",
        action="store_true",
        help="Send immediately instead of waiting until 2026-05-08 12:00 MSK.",
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
        help="Send the broadcast photo message only to ADMIN_ID immediately.",
    )
    args = parser.parse_args()

    try:
        if args.test_admin:
            await send_test_message()
            return

        if not args.now and not args.dry_run:
            await wait_until(DEFAULT_RUN_AT)

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
