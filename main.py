from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError

from config import settings
from db.db_helper import db_helper
from db.models import Base
from loader import bot, dp, logger, redis
from rabbitmq import close_rabbitmq, init_rabbitmq, publish_update


async def init_db():
    async with db_helper.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    webhook_url = settings.BASE_URL + "/rpp/webhook"
    await init_db()
    await init_rabbitmq()
    webhook_kwargs = {
        "url": webhook_url,
        "secret_token": settings.SECRET_TG_KEY,
        "drop_pending_updates": False,
    }
    if settings.WEBHOOK_IP_ADDRESS is not None:
        webhook_kwargs["ip_address"] = settings.WEBHOOK_IP_ADDRESS

    await bot.set_webhook(
        **webhook_kwargs,
    )
    logger.info("Webhook set to: %s", webhook_url)

    yield

    await close_rabbitmq()
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
        await publish_update(update_data)
        return {"status": "ok"}
    except (RedisConnectionError, RuntimeError) as exc:
        logger.error("Webhook ingress unavailable: %s", exc)
        return JSONResponse({"status": "degraded"}, status_code=503)
    except Exception as exc:
        logger.error("Telegram webhook error: %s", exc, exc_info=True)
        return JSONResponse({"status": "error"}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, log_level="info")
