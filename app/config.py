from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import yaml
import os

from .secrets import load_secrets

load_secrets()

class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    TG_CHAT_ID: int | None = None
    MONITORING_SLACK_WEBHOOK: str | None = None
    MONITORING_TELEGRAM_TOKEN: str | None = None
    MONITORING_TELEGRAM_CHAT_ID: int | None = None

    PROXY_URL: str | None = None
    DATA_ENCRYPTION_KEY: str

    DB_URL: str = "sqlite+aiosqlite:///./deals.db"

    SCRAPE_CONCURRENCY: int = 2
    DEFAULT_GEOID: str = "213"

    MIN_DISCOUNT: int = 25
    MIN_SCORE: int = 70
    DAILY_MSG_LIMIT: int = 20

    SHIPPING_COST: int = 199

    BUDGET_MAX_PAGES: int = 100
    BUDGET_MAX_TASKS: int = 20
    QUIET_HOURS: str | None = None

    PRESETS_FILE: str = "./presets.yaml"

    REDIS_URL: str = "redis://localhost:6379/0"
    QUEUE_STREAM: str = "presets"

    S3_BUCKET: str | None = None
    S3_ENDPOINT: str | None = None
    S3_ACCESS_KEY: str | None = None
    S3_SECRET_KEY: str | None = None
    SNAPSHOT_TTL_DAYS: int = 7

    METRICS_PORT: int = 8000
    SENTRY_DSN: str | None = None
    DLQ_OVERFLOW_THRESHOLD: int = 100

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

class PresetItem(BaseModel):
    name: str
    url: str

class Presets(BaseModel):
    geoid_default: str = Field(default="213")
    sites: dict

def load_presets(path: str) -> Presets:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Presets file not found: {path}")
    with p.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return Presets(**data)

settings = Settings()  # load at import time
presets = load_presets(settings.PRESETS_FILE)
