import asyncio
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from uuid import uuid4
import time

import sentry_sdk

import boto3
import redis.asyncio as redis
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from ..config import settings
from ..metrics import render_latency, render_errors

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
        if self._s3_bucket:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=getattr(settings, "S3_ENDPOINT", None),
                aws_access_key_id=getattr(settings, "S3_ACCESS_KEY", None),
                aws_secret_access_key=getattr(settings, "S3_SECRET_KEY", None),
            )
        else:
            self._s3 = None
        self._snapshot_ttl = getattr(settings, "SNAPSHOT_TTL_DAYS", 7)

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

    async def save_snapshot(self, url: str, html: str, screenshot: bytes, prefix: str = "errors") -> None:
        if not self._s3:
            return
        parsed = urlparse(url)
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base = f"{prefix}/{parsed.netloc}/{stamp}-{uuid4()}"
        expires = datetime.utcnow() + timedelta(days=self._snapshot_ttl)
        try:
            await asyncio.gather(
                asyncio.to_thread(
                    self._s3.put_object,
                    Bucket=self._s3_bucket,
                    Key=f"{base}.html",
                    Body=html.encode("utf-8"),
                    ContentType="text/html",
                    Expires=expires,
                ),
                asyncio.to_thread(
                    self._s3.put_object,
                    Bucket=self._s3_bucket,
                    Key=f"{base}.png",
                    Body=screenshot,
                    ContentType="image/png",
                    Expires=expires,
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
        sleep_jitter_ms: int = 1000,
    ) -> tuple[str, bytes]:
        """Возвращает (html, screenshot_png)"""
        assert self._browser, "RenderService not started"
        domain = urlparse(url).netloc
        sem = self._domain_sems.setdefault(domain, asyncio.Semaphore(self._per_domain))
        cache_key = f"render:{url}"
        meta_key = f"{cache_key}:meta"
        if self._redis and cache_ttl is None:
            cache_ttl = random.randint(30, 180)
        cached_html: Optional[str] = None
        cached_meta: dict[str, str] | None = None
        if self._redis:
            cached_html_raw = await self._redis.get(cache_key)
            if cached_html_raw:
                return cached_html_raw.decode(), b""
            meta_raw = await self._redis.get(meta_key)
            if meta_raw:
                try:
                    cached_meta = json.loads(meta_raw)
                    cached_html = cached_meta.get("html")
                    etag = etag or cached_meta.get("etag")
                    last_modified = last_modified or cached_meta.get("last_modified")
                except Exception:
                    cached_meta = None

        start = time.perf_counter()
        try:
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
                        resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                        if wait_selector:
                            try:
                                await page.wait_for_selector(wait_selector, timeout=timeout_ms // 2)
                            except Exception:
                                html = await page.content()
                                screenshot = await page.screenshot(full_page=True)
                                await self.save_snapshot(url, html, screenshot)
                                raise
                        await page.wait_for_timeout(sleep_ms + random.randint(0, sleep_jitter_ms))
                        status = resp.status if resp else 200
                        if status == 304 and cached_html:
                            if self._redis:
                                await self._redis.set(cache_key, cached_html, ex=cache_ttl)
                            return cached_html, b""
                        html = await page.content()
                        screenshot = await page.screenshot(full_page=True)
                        if self._redis and cache_ttl:
                            await self._redis.set(cache_key, html, ex=cache_ttl)
                            try:
                                meta = {
                                    "html": html,
                                    "etag": resp.headers.get("etag") if resp else None,
                                    "last_modified": resp.headers.get("last-modified") if resp else None,
                                }
                                await self._redis.set(meta_key, json.dumps(meta), ex=86400)
                            except Exception:
                                pass
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
                        await self.save_snapshot(url, html, screenshot)
                        raise
                    finally:
                        await page.close()
                finally:
                    await self._reset_context(ctx)
                    await self._ctx_pool.put(ctx)
        except Exception as e:
            render_errors.labels(domain=domain).inc()
            sentry_sdk.capture_exception(e)
            raise
        finally:
            render_latency.labels(domain=domain).observe(time.perf_counter() - start)
