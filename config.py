from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    ASYNC_DB_URL: str | None = None
    DB_URL: str | None = None
    BOT_TOKEN: SecretStr
    REDIS_URL: str | None = None
    RABBITMQ_URL: str | None = None
    BASE_URL: str
    WEBHOOK_IP_ADDRESS: str | None = None
    ADMIN_ID: int
    CHAT_ID_TO_CHECK: int
    CHAT_URL: str
    SECOND_CHAT_ID_TO_CHECK: int
    SECOND_CHAT_URL: str
    SECRET_TG_KEY: str
    YDISK_LINK: str

    HOST: str
    PORT: int
    RABBITMQ_PREFETCH: int = 1
    RABBITMQ_MAX_RETRIES: int = 5
    RABBITMQ_RETRY_DELAY_MS: int = 30000

    # PROXY
    PROXY_IP_OR_DOMAIN: str
    PROXY_PORT: int
    PROXY_LOGIN: str
    PROXY_PASSWORD: SecretStr

    @property
    def database_url(self) -> str:
        if self.ASYNC_DB_URL:
            return self.ASYNC_DB_URL
        if self.DB_URL:
            if self.DB_URL.startswith("sqlite:///"):
                return self.DB_URL.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
            return self.DB_URL
        return f"sqlite+aiosqlite:///{BASE_DIR / 'rpp_tg_bot.db'}"

    @property
    def REDIS_HOST(self) -> str:
        if not self.REDIS_URL:
            return "localhost"
        parsed = urlparse(self.REDIS_URL)
        return parsed.hostname or "localhost"

    @property
    def REDIS_PORT(self) -> int:
        if not self.REDIS_URL:
            return 6379
        parsed = urlparse(self.REDIS_URL)
        return parsed.port or 6379

    @property
    def REDIS_DB(self) -> int:
        if not self.REDIS_URL:
            return 0
        parsed = urlparse(self.REDIS_URL)
        path = (parsed.path or "/0").lstrip("/")
        return int(path) if path.isdigit() else 0

    @property
    def REDIS_PASSWORD(self) -> str | None:
        if not self.REDIS_URL:
            return None
        parsed = urlparse(self.REDIS_URL)
        return parsed.password

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / "rpp.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("WEBHOOK_IP_ADDRESS", mode="before")
    @classmethod
    def empty_strings_to_none(cls, value):
        if value == "":
            return None
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
