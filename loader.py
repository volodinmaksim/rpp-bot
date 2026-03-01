import logging
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis.asyncio import Redis

from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

bot = Bot(token=settings.BOT_TOKEN.get_secret_value())


if settings.REDIS_URL:
    redis = Redis.from_url(settings.REDIS_URL)
    logger.info("FSM storage: Redis (%s)", settings.REDIS_URL)
    dp = Dispatcher(storage=RedisStorage(redis=redis))
else:
    redis = None
    logger.warning("FSM storage: MemoryStorage (REDIS_URL is not set)")
    dp = Dispatcher(storage=MemoryStorage())

if settings.REDIS_URL:
    scheduler_jobstores = {
        "default": RedisJobStore(
            jobs_key="apscheduler.jobs",
            run_times_key="apscheduler.run_times",
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
        )
    }
    logger.info(
        "Scheduler jobstore: Redis (%s:%s/%s)",
        settings.REDIS_HOST,
        settings.REDIS_PORT,
        settings.REDIS_DB,
    )
else:
    scheduler_jobstores = {"default": MemoryJobStore()}
    logger.warning("Scheduler jobstore: MemoryJobStore (REDIS_URL is not set)")

scheduler = AsyncIOScheduler(timezone=ZoneInfo("UTC"), jobstores=scheduler_jobstores)
