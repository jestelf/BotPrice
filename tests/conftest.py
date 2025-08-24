import os

DEFAULT_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("DATA_ENCRYPTION_KEY", DEFAULT_KEY)
