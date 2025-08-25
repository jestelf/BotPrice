import types
import pytest

from app.scraper.render import RenderService


@pytest.mark.asyncio
async def test_throttle_on_errors(monkeypatch):
    rs = RenderService()
    domain = "example.com"
    rs._error_times[domain] = [90, 95, 96, 97]

    captured = {"delay": 0}

    async def fake_sleep(v):
        captured["delay"] += v

    monkeypatch.setattr("app.scraper.render.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("app.scraper.render.time.time", lambda: 100)

    await rs._throttle(domain)
    assert captured["delay"] == 1
