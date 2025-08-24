import json
import logging
import urllib.request
from typing import Optional

from ..config import settings


logger = logging.getLogger(__name__)

_FAIL_THRESHOLD = 3
_slack_failures = 0


def _post(url: str, payload: dict) -> bool:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception:
        logger.exception("Ошибка отправки webhook: %s", url)
        return False


def notify_slack(text: str) -> bool:
    webhook: Optional[str] = getattr(settings, "MONITORING_SLACK_WEBHOOK", None)
    if webhook:
        return _post(webhook, {"text": text})
    return False


def notify_telegram(text: str) -> bool:
    token: Optional[str] = getattr(settings, "MONITORING_TELEGRAM_TOKEN", None)
    chat_id: Optional[int] = getattr(settings, "MONITORING_TELEGRAM_CHAT_ID", None)
    if token and chat_id:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        return _post(url, {"chat_id": chat_id, "text": text})
    return False


def notify_monitoring(text: str) -> bool:
    """Отправляет сообщение в каналы мониторинга."""
    global _slack_failures
    ok = notify_slack(text)
    if ok:
        _slack_failures = 0
        return True
    _slack_failures = min(_slack_failures + 1, _FAIL_THRESHOLD)
    if _slack_failures >= _FAIL_THRESHOLD:
        return notify_telegram(text)
    return False
