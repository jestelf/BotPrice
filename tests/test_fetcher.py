import asyncio
from aiohttp import web
import pytest

from render_pool.fetcher import Fetcher


@pytest.mark.asyncio
async def test_fetcher_respects_robots_and_cache():
    calls = {"small": 0}

    async def robots_handler(request):
        return web.Response(text="User-agent: *\nDisallow: /blocked")

    async def ok_handler(request):
        return web.Response(text="hello world")

    async def small_handler(request):
        calls["small"] += 1
        if calls["small"] == 1:
            return web.Response(text="hello world")
        return web.Response(text="hello world!")

    async def blocked_handler(request):
        return web.Response(text="nope")

    app = web.Application()
    app.router.add_get("/robots.txt", robots_handler)
    app.router.add_get("/ok", ok_handler)
    app.router.add_get("/small", small_handler)
    app.router.add_get("/blocked", blocked_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    base = f"http://localhost:{port}"

    fetcher = Fetcher()
    try:
        # обычный запрос
        html = await fetcher.fetch(f"{base}/ok")
        assert html == "hello world"
        # robots.txt запрещает /blocked
        with pytest.raises(PermissionError):
            await fetcher.fetch(f"{base}/blocked")
        # небольшое изменение контента => возвращается кэш
        first = await fetcher.fetch(f"{base}/small")
        second = await fetcher.fetch(f"{base}/small")
        assert first == "hello world"
        assert second == first
    finally:
        await fetcher.close()
        await runner.cleanup()

