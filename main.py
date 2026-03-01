from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from collections import deque

from config import settings
from redis.exceptions import ConnectionError as RedisConnectionError

from db.db_helper import db_helper
from db.models import Base
from loader import bot, dp, logger, scheduler, redis
from routers import (
    start_router,
    onboarding_router,
    pro_router,
    survey_router,
    novice_router,
    pro_continued_router,
    novice_continued_router,
)

PROCESSED_UPDATES_LIMIT = 5000
_processed_update_ids_queue: deque[int] = deque(maxlen=PROCESSED_UPDATES_LIMIT)
_processed_update_ids: set[int] = set()


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
        drop_pending_updates=True,
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

        if update_id in _processed_update_ids:
            logger.info("Duplicate update skipped: %s", update.update_id)
            return {"status": "ok"}

        await dp.feed_update(bot, update)

        if len(_processed_update_ids_queue) == PROCESSED_UPDATES_LIMIT:
            stale_update_id = _processed_update_ids_queue.popleft()
            _processed_update_ids.discard(stale_update_id)

        _processed_update_ids_queue.append(update_id)
        _processed_update_ids.add(update_id)

        return {"status": "ok"}
    except RedisConnectionError as e:
        logger.error("Redis is unavailable while processing update: %s", e)
        return {"status": "degraded"}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}", exc_info=True)
        return JSONResponse({"status": "error"}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, log_level="info")
