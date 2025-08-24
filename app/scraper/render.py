import asyncio
from typing import Dict, Any, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

class RenderService:
    def __init__(self, headless: bool = True):
        self._headless = headless
        self._pw = None
        self._browser: Optional[Browser] = None
        self._lock = asyncio.Lock()

    async def start(self):
        if self._browser:
            return
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=True, args=["--no-sandbox"])

    async def stop(self):
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._pw:
            await self._pw.stop()
            self._pw = None

    async def fetch(
        self,
        url: str,
        cookies: list[Dict[str, Any]] | None = None,
        wait_selector: str | None = None,
        extra_headers: Dict[str, str] | None = None,
        region_hint: str | None = None,
        timeout_ms: int = 60000,
        sleep_ms: int = 2000,
    ) -> tuple[str, bytes]:
        """
        Returns (html, screenshot_png)
        """
        assert self._browser, "RenderService not started"
        async with self._lock:
            ctx = await self._browser.new_context(user_agent=DEFAULT_UA, viewport={"width": 1366, "height": 860})
            if extra_headers:
                await ctx.set_extra_http_headers(extra_headers)
            if cookies:
                await ctx.add_cookies(cookies)
            page: Page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=timeout_ms // 2)
                except Exception:
                    pass
            await page.wait_for_timeout(sleep_ms)
            html = await page.content()
            screenshot = await page.screenshot(full_page=True)
            await ctx.close()
            return html, screenshot
