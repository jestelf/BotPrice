import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import redis.asyncio as redis
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from ..config import settings

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


class RenderService:
    def __init__(self, headless: bool = True, ctx_pool: int = 4, per_domain: int = 2):
        self._headless = headless
        self._pw = None
        self._browser: Optional[Browser] = None
        self._ctx_pool: asyncio.Queue[BrowserContext] = asyncio.Queue(maxsize=ctx_pool)
        self._domain_sems: dict[str, asyncio.Semaphore] = {}
        self._per_domain = per_domain
        self._redis = redis.from_url(settings.REDIS_URL)
        self._s3_bucket = getattr(settings, "S3_BUCKET", None)
        self._s3 = boto3.client("s3") if self._s3_bucket else None

    async def start(self):
        if self._browser:
            return
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=self._headless, args=["--no-sandbox"])
        for _ in range(self._ctx_pool.maxsize):
            ctx = await self._browser.new_context(user_agent=DEFAULT_UA, viewport={"width": 1366, "height": 860})
            await self._ctx_pool.put(ctx)

    async def stop(self):
        while not self._ctx_pool.empty():
            ctx = await self._ctx_pool.get()
            await ctx.close()
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._pw:
            await self._pw.stop()
            self._pw = None
        if self._redis:
            await self._redis.close()

    async def _reset_context(self, ctx: BrowserContext) -> None:
        try:
            await ctx.clear_cookies()
            await ctx.set_extra_http_headers({})
            await ctx.set_storage_state({"origins": []})
        except Exception:
            pass

    async def _upload_debug(self, url: str, html: str, screenshot: bytes) -> None:
        if not self._s3:
            return
        parsed = urlparse(url)
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base = f"errors/{parsed.netloc}/{stamp}-{uuid4()}"
        try:
            await asyncio.gather(
                asyncio.to_thread(
                    self._s3.put_object,
                    Bucket=self._s3_bucket,
                    Key=f"{base}.html",
                    Body=html.encode("utf-8"),
                    ContentType="text/html",
                ),
                asyncio.to_thread(
                    self._s3.put_object,
                    Bucket=self._s3_bucket,
                    Key=f"{base}.png",
                    Body=screenshot,
                    ContentType="image/png",
                ),
            )
        except Exception:
            pass

    async def fetch(
        self,
        url: str,
        cookies: list[Dict[str, Any]] | None = None,
        wait_selector: str | None = None,
        extra_headers: Dict[str, str] | None = None,
        region_hint: str | None = None,
        timeout_ms: int = 60000,
        sleep_ms: int = 2000,
        cache_ttl: int | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> tuple[str, bytes]:
        """Возвращает (html, screenshot_png)"""
        assert self._browser, "RenderService not started"
        domain = urlparse(url).netloc
        sem = self._domain_sems.setdefault(domain, asyncio.Semaphore(self._per_domain))
        cache_key = None
        if cache_ttl and self._redis:
            cache_key = f"render:{url}"
            cached = await self._redis.get(cache_key)
            if cached:
                return cached.decode(), b""

        async with sem:
            ctx = await self._ctx_pool.get()
            try:
                headers = dict(extra_headers or {})
                if etag:
                    headers["If-None-Match"] = etag
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
                if headers:
                    await ctx.set_extra_http_headers(headers)
                if cookies:
                    await ctx.add_cookies(cookies)
                if region_hint:
                    await ctx.add_cookies([
                        {
                            "name": "region",
                            "value": region_hint,
                            "domain": f".{domain}",
                            "path": "/",
                        }
                    ])
                page: Page = await ctx.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    if wait_selector:
                        try:
                            await page.wait_for_selector(wait_selector, timeout=timeout_ms // 2)
                        except Exception:
                            html = await page.content()
                            screenshot = await page.screenshot(full_page=True)
                            await self._upload_debug(url, html, screenshot)
                            raise
                    await page.wait_for_timeout(sleep_ms)
                    html = await page.content()
                    screenshot = await page.screenshot(full_page=True)
                    if cache_key:
                        await self._redis.set(cache_key, html, ex=cache_ttl)
                    return html, screenshot
                except Exception:
                    try:
                        html = await page.content()
                    except Exception:
                        html = ""
                    try:
                        screenshot = await page.screenshot(full_page=True)
                    except Exception:
                        screenshot = b""
                    await self._upload_debug(url, html, screenshot)
                    raise
                finally:
                    await page.close()
            finally:
                await self._reset_context(ctx)
                await self._ctx_pool.put(ctx)
