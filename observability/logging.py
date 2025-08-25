import logging
import os
import json
import re

import sentry_sdk


_PII_PATTERNS = [
    re.compile(r"[^\s]+@[^\s]+"),
    re.compile(r"token[^\s]*", re.I),
    re.compile(r"chat_id[^\s]*", re.I),
    re.compile(r"\b\d{6,}\b"),
]


def _redact(text: str) -> str:
    for pat in _PII_PATTERNS:
        text = pat.sub("<pii>", text)
    return text


class PiiFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - trivial
        msg = record.getMessage()
        record.msg = _redact(msg)
        record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - trivial
        data = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


def setup_logging() -> None:
    """Configure JSON logging and initialize Sentry."""
    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn:
        sentry_sdk.init(sentry_dsn)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    root.addFilter(PiiFilter())
