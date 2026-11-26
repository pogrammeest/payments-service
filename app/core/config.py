from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "dev-api-key"

    gateway_url: str = "http://localhost:8001"
    gateway_timeout: float = 10.0
    webhook_timeout: float = 5.0

    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100

    max_attempts: int = 3  # всего попыток, включая первую
    # сколько сообщений consumer держит в обработке одновременно; меньше пула БД (5 + 10)
    consumer_prefetch: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
