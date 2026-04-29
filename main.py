from contextlib import asynccontextmanager

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from config import settings
from db.db_helper import db_helper
from db.models import Base
from loader import bot, dp, logger, redis


async def init_db():
    async with db_helper.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("HTTP app started in polling mode")

    yield

    await dp.storage.close()
    if redis is not None:
        await redis.aclose()
    logger.info("HTTP app stopped")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def healthcheck():
    return {"status": "ok", "mode": "polling"}


@app.api_route("/rpp/webhook", methods=["GET", "POST", "HEAD"])
async def handle_telegram_webhook():
    # Webhook path stays reserved so rollback to webhook mode is straightforward.
    return JSONResponse(
        {"status": "polling_mode"},
        status_code=status.HTTP_410_GONE,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, log_level="info")
