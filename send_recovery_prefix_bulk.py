import argparse
import asyncio
import logging
from pathlib import Path

from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
)

from loader import bot

logger = logging.getLogger(__name__)

RECOVERY_PREFIX = (
    "Привет. Из-за замедления Telegram ответ на кнопку мог не обработаться с первого раза.\n\n"
    "Если вы уже пытались ответить, пожалуйста, выберите еще раз. Если еще не пытались, извините за беспокойство."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send recovery prefix to a list of Telegram users."
    )
    parser.add_argument(
        "--ids-file",
        type=Path,
        required=True,
        help="Text file with one tg_id per line.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print recipients without sending messages.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between messages.",
    )
    return parser.parse_args()


def load_ids(path: Path) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        tg_id = int(line)
        if tg_id in seen:
            continue
        seen.add(tg_id)
        ids.append(tg_id)

    return ids


async def send_messages(tg_ids: list[int], *, dry_run: bool, delay: float) -> tuple[int, int]:
    sent = 0
    failed = 0

    for tg_id in tg_ids:
        logger.info("Recipient tg_id=%s", tg_id)

        if dry_run:
            continue

        try:
            await bot.send_message(chat_id=tg_id, text=RECOVERY_PREFIX)
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramAPIError) as exc:
            failed += 1
            logger.warning("Failed to send message to tg_id=%s: %s", tg_id, exc)
        except Exception:
            failed += 1
            logger.exception("Unexpected error while sending to tg_id=%s", tg_id)

        if delay > 0:
            await asyncio.sleep(delay)

    return sent, failed


async def async_main() -> None:
    args = parse_args()
    tg_ids = load_ids(args.ids_file)
    logger.info("Loaded %s unique recipients", len(tg_ids))

    try:
        sent, failed = await send_messages(
            tg_ids,
            dry_run=args.dry_run,
            delay=args.delay,
        )
        logger.info(
            "Bulk send finished. dry_run=%s sent=%s failed=%s",
            args.dry_run,
            sent,
            failed,
        )
    finally:
        await bot.session.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s %(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
