import asyncio
import types
from pathlib import Path
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scraper import render as render_module
from app.scraper.render import RenderService


@pytest.mark.asyncio
async def test_fetch_error_creates_snapshot_and_metrics(monkeypatch):
    svc = RenderService()
    svc._browser = object()
    svc._redis = None

    ctx = AsyncMock()
    page = AsyncMock()
    ctx.new_page.return_value = page
    svc._ctx_pool = asyncio.Queue()
    await svc._ctx_pool.put(ctx)

    resp = types.SimpleNamespace(status=200, headers={})
    page.goto.return_value = resp
    page.wait_for_selector.side_effect = Exception("boom")
    page.content.return_value = "<html></html>"
    page.screenshot.return_value = b"img"

    save_snapshot = AsyncMock()
    monkeypatch.setattr(svc, "save_snapshot", save_snapshot)

    inc_mock = MagicMock()
    labels_mock = MagicMock(return_value=types.SimpleNamespace(inc=inc_mock))
    monkeypatch.setattr(render_module, "render_errors", types.SimpleNamespace(labels=labels_mock))
    monkeypatch.setattr(render_module.sentry_sdk, "capture_exception", lambda e: None)

    with pytest.raises(Exception):
        await svc.fetch("https://example.com", wait_selector="#sel")

    assert save_snapshot.call_count >= 1
    assert inc_mock.call_count == 1
