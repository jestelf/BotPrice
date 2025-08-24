import sys
from pathlib import Path
import urllib.error
import urllib.request

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_slack_failure_logs_and_fallback(monkeypatch, caplog):
    import app.notifier.monitoring as mon

    mon.settings.MONITORING_SLACK_WEBHOOK = "https://slack.test"
    mon.settings.MONITORING_TELEGRAM_TOKEN = "tg"
    mon.settings.MONITORING_TELEGRAM_CHAT_ID = 1

    calls: list[str] = []

    def fake_urlopen(req, timeout=5):
        calls.append(req.full_url)
        if req.full_url == "https://slack.test":
            raise urllib.error.URLError("fail")
        class Dummy:
            pass
        return Dummy()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    caplog.set_level("ERROR")
    for _ in range(mon._FAIL_THRESHOLD):
        mon.notify_monitoring("msg")

    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == mon._FAIL_THRESHOLD
    assert any("slack.test" in r.message for r in errors)

    assert any("api.telegram.org" in url for url in calls)
