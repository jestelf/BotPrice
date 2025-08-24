from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import yaml
import os

class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    TG_CHAT_ID: int | None = None

    DB_URL: str = "sqlite+aiosqlite:///./deals.db"

    SCRAPE_CONCURRENCY: int = 2
    DEFAULT_GEOID: str = "213"

    MIN_DISCOUNT: int = 25
    MIN_SCORE: int = 70

    PRESETS_FILE: str = "./presets.yaml"

    REDIS_URL: str = "redis://localhost:6379/0"
    QUEUE_STREAM: str = "presets"

    S3_BUCKET: str | None = None

    model_config = SettingsConfigModel = SettingsConfigDict(env_file='.env', extra='ignore')

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
