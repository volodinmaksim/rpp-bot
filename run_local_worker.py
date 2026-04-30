import asyncio

import config


config.settings.ASYNC_DB_URL = "sqlite+aiosqlite:///rpp_tg_bot.db"
config.settings.REDIS_URL = None

import worker  # noqa: E402


if __name__ == "__main__":
    asyncio.run(worker.main())
