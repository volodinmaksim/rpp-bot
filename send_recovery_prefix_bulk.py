import argparse
import asyncio
import logging

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

TG_IDS = [
    8292814786, 388364212, 129008217, 732796480, 384604536, 1013004594, 390490597,
    538970906, 1163076567, 238264843, 1272369320, 417598805, 686253401, 411337232,
    70195204, 870613052, 531211843, 1779199610, 6164991553, 1073845275, 187561854,
    388045363, 199168095, 339185363, 958474344, 230108844, 1622315172, 825261056,
    1097927499, 1991285102, 440205763, 474716146, 5844178106, 1003390841, 1000204501,
    859227677, 518857406, 537282606, 112687512, 507008593, 2874355, 1003421783,
    313917894, 1897609773, 676830763, 323431395, 783056348, 415748090, 2131653472,
    1004421521, 391057274, 839312462, 96344307, 1805484842, 293881100, 460657516,
    1899329846, 5240302626, 6929654641, 860749721, 1475879535, 968800353, 785352966,
    490210648, 1166013362, 1155572911, 424023706, 154812155, 363981039, 181743410,
    542304515, 896788351, 476522689, 362857308, 585079562, 451503194, 1551000016,
    1804606133, 907758602, 1120186960, 185728436, 176033781, 831535074, 1280159176,
    808745072, 85429688, 146871078, 450737331, 101616429, 441699290, 68210224,
    422863895, 783786381, 174845637, 598653715, 132173981, 253072078, 464805171,
    287815700, 328419614, 656081347, 5296841468, 122760945, 752611759, 345747310,
    384591424, 899513522, 334690188, 323657025, 583091941, 183034430, 1124333937,
    887978723, 382126065, 337618667, 7844394200, 860742113, 1019033732, 442020824,
    6148587797, 565660847, 168271505, 939290173, 333853701, 677775827, 266392806,
    1321582211, 186283964, 411437310, 1194377624, 316517295, 588252902, 822754450,
    349385657, 342405407, 684218940, 7315201605, 334433938, 349898965, 518870960,
    485054417, 8161177403, 324932438, 717367758, 171218361, 429490559, 340570821,
    228963449, 561229156, 475624895, 855431448, 139016806, 154629073, 622261709,
    57569724, 861690065, 851521992, 460831199, 362247134, 524973143, 452077442,
    861688613, 447251247, 1305404169, 1108664145, 99722524, 163522764, 1041746603,
    485705272, 711183099, 762520158, 447456919, 394752512, 449577643, 844701410,
    440606599, 820537875, 375398897, 296250420, 834670252, 502973945, 1611193490,
    487565965, 138892668, 872626195, 376759742, 752406218, 1109954723, 494687868,
    776902423, 887570632, 77098564, 694469829, 1186864204, 127455626, 1149030194,
    52824279, 880811094, 864454995, 128796525, 911632454, 6488835416, 450298161,
    905111097, 884960257, 869699501, 396748763, 96895844, 843567028, 366333589,
    535657330, 1041555897, 1347802273, 5173854497, 453188930, 610209314, 443331602,
    209049535, 1022718352,
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send recovery prefix to embedded Telegram user ids."
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
    logger.info("Loaded %s embedded recipients", len(TG_IDS))

    try:
        sent, failed = await send_messages(
            TG_IDS,
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
