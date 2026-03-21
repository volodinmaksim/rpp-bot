import asyncio
import contextlib
import json
import signal

from aiogram.types import Update

from db.db_helper import db_helper
from db.models import Base
from loader import bot, dp, logger, redis, scheduler
from rabbitmq import (
    close_rabbitmq,
    consume_updates,
    handle_worker_failure,
    mark_update_processed,
    release_processing_update,
    try_acquire_update,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from routers import (
    novice_continued_router,
    novice_router,
    onboarding_router,
    pro_continued_router,
    pro_router,
    start_router,
    survey_router,
)


async def init_db() -> None:
    async with db_helper.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def register_routers() -> None:
    dp.include_router(start_router)
    dp.include_router(onboarding_router)
    dp.include_router(novice_router)
    dp.include_router(survey_router)
    dp.include_router(pro_router)
    dp.include_router(pro_continued_router)
    dp.include_router(novice_continued_router)


async def handle_message(message) -> None:
    payload = json.loads(message.body.decode("utf-8"))
    update_id = int(payload["update_id"])

    try:
        if not await try_acquire_update(update_id):
            logger.info("Duplicate queued update skipped: %s", update_id)
            await message.ack()
            return

        update = Update.model_validate(payload["payload"])
        await dp.feed_update(bot, update)
        await mark_update_processed(update_id)
        await message.ack()
    except RedisConnectionError as exc:
        logger.error("Redis is unavailable while consuming update %s: %s", update_id, exc)
        await release_processing_update(update_id)
        await message.nack(requeue=True)
    except Exception as exc:
        logger.error("Worker failed for update %s: %s", update_id, exc, exc_info=True)
        await handle_worker_failure(message, payload, exc)


async def main() -> None:
    await init_db()
    register_routers()
    scheduler.start()
    await consume_updates(handle_message)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(close_rabbitmq())
        with contextlib.suppress(Exception):
            scheduler.shutdown()
        with contextlib.suppress(Exception):
            asyncio.run(dp.storage.close())
        with contextlib.suppress(Exception):
            if redis is not None:
                asyncio.run(redis.aclose())
