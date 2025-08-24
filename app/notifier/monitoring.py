import json
import urllib.request
from typing import Optional

from ..config import settings


def _post(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def notify_slack(text: str) -> None:
    webhook: Optional[str] = getattr(settings, "MONITORING_SLACK_WEBHOOK", None)
    if webhook:
        _post(webhook, {"text": text})


def notify_telegram(text: str) -> None:
    token: Optional[str] = getattr(settings, "MONITORING_TELEGRAM_TOKEN", None)
    chat_id: Optional[int] = getattr(settings, "MONITORING_TELEGRAM_CHAT_ID", None)
    if token and chat_id:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        _post(url, {"chat_id": chat_id, "text": text})


def notify_monitoring(text: str) -> None:
    """Отправляет сообщение в каналы мониторинга."""
    notify_slack(text)
    notify_telegram(text)
