import asyncio
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import settings
from db.db_helper import db_helper
from db.models import Base
from loader import bot, dp, logger, scheduler, redis
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

PROCESSED_UPDATES_LIMIT = 5000
_processed_update_ids_queue: deque[int] = deque(maxlen=PROCESSED_UPDATES_LIMIT)
_processed_update_ids: set[int] = set()
_processing_update_ids: set[int] = set()
_background_tasks: set[asyncio.Task] = set()


async def init_db():
    async with db_helper.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    webhook_url = settings.BASE_URL + "/rpp/webhook"
    await init_db()
    dp.include_router(start_router)
    dp.include_router(onboarding_router)
    dp.include_router(novice_router)
    dp.include_router(survey_router)
    dp.include_router(pro_router)
    dp.include_router(pro_continued_router)
    dp.include_router(novice_continued_router)
    scheduler.start()
    await bot.set_webhook(
        url=webhook_url,
        secret_token=settings.SECRET_TG_KEY,
        drop_pending_updates=False,
    )
    logger.info(f"Webhook set to: {webhook_url}")

    yield

    scheduler.shutdown()
    await dp.storage.close()
    if redis is not None:
        await redis.aclose()
    await bot.delete_webhook()
    logger.info("Webhook deleted")


app = FastAPI(lifespan=lifespan)


def _track_background_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _process_update_in_background(update) -> None:
    update_id = update.update_id
    try:
        await dp.feed_update(bot, update)
        if len(_processed_update_ids_queue) == PROCESSED_UPDATES_LIMIT:
            stale_update_id = _processed_update_ids_queue.popleft()
            _processed_update_ids.discard(stale_update_id)

        _processed_update_ids_queue.append(update_id)
        _processed_update_ids.add(update_id)
    except RedisConnectionError as exc:
        logger.error("Redis is unavailable while processing update: %s", exc)
    except Exception as exc:
        logger.error("Telegram update processing error: %s", exc, exc_info=True)
    finally:
        _processing_update_ids.discard(update_id)


@app.post("/rpp/webhook")
async def handle_telegram_webhook(request: Request):
    try:
        secret_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret_token != settings.SECRET_TG_KEY:
            logger.warning("Invalid Telegram webhook secret token")
            return JSONResponse({"status": "forbidden"}, status_code=403)

        update_data = await request.json()
        from aiogram.types import Update

        update = Update.model_validate(update_data)
        update_id = update.update_id

        if update_id in _processed_update_ids or update_id in _processing_update_ids:
            logger.info("Duplicate update skipped: %s", update_id)
            return {"status": "ok"}

        _processing_update_ids.add(update_id)
        _track_background_task(asyncio.create_task(_process_update_in_background(update)))

        return {"status": "ok"}
    except Exception as exc:
        logger.error("Telegram webhook error: %s", exc, exc_info=True)
        return JSONResponse({"status": "error"}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, log_level="info")
