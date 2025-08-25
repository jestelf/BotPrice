from typing import Iterable
import time
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from history.service import update_product_metrics

import sentry_sdk

from ..scraper.render import RenderService
from ..scraper.adapters import ozon as ozon_ad, market as market_ad
from ..schemas import OfferRaw, OfferNormalized
from ..processing.normalize import normalize
from ..processing.score import discount_pct, compute_score
from ..processing.detectors import is_fake_msrp
from ..processing.dedupe import dedupe_offers
from ..models import Product, Offer, PriceHistory
from ..config import settings
from ..metrics import update_listing_stats, update_category_price_stats
from observability.metrics import parse_latency, parse_errors

async def fetch_site_list(
    render: RenderService, site: str, url: str, geoid: str | None
) -> list[OfferRaw]:
    geoid_actual = geoid or settings.DEFAULT_GEOID
    domain = urlparse(url).netloc
    if site == "ozon":
        cookies = ozon_ad.region_cookies(geoid_actual)
        html, screenshot = await render.fetch(
            url=url,
            cookies=cookies,
            wait_selector='[data-widget="searchResultsV2"]',
            region_hint=geoid,
        )
        if not ozon_ad.ensure_region(html, geoid_actual):
            raise ValueError("Не удалось выбрать регион")
        start = time.perf_counter()
        try:
            items = ozon_ad.parse_listing(html)
            parse_latency.labels(domain=domain).observe(time.perf_counter() - start)
        except Exception as e:
            parse_errors.labels(domain=domain).inc()
            await render.save_snapshot(url, html, screenshot, prefix="schema")
            sentry_sdk.capture_exception(e)
            raise
        if not items:
            await render.save_snapshot(url, html, screenshot, prefix="schema")
        update_listing_stats(domain, not items)
        return items
    elif site == "market":
        cookies = market_ad.region_cookies(geoid_actual)
        html, screenshot = await render.fetch(
            url=url,
            cookies=cookies,
            wait_selector="article[data-autotest-id='product-snippet']",
            region_hint=geoid,
        )
        if not market_ad.ensure_region(html, geoid_actual):
            raise ValueError("Не удалось выбрать регион")
        start = time.perf_counter()
        try:
            items = market_ad.parse_listing(html, geoid=geoid)
            parse_latency.labels(domain=domain).observe(time.perf_counter() - start)
        except Exception as e:
            parse_errors.labels(domain=domain).inc()
            await render.save_snapshot(url, html, screenshot, prefix="schema")
            sentry_sdk.capture_exception(e)
            raise
        if not items:
            await render.save_snapshot(url, html, screenshot, prefix="schema")
        update_listing_stats(domain, not items)
        return items
    else:
        return []

async def upsert_offer(session: AsyncSession, item: OfferNormalized):
    # Product
    q = select(Product).where(Product.url == item.url)
    res = await session.execute(q)
    prod: Product | None = res.scalar_one_or_none()
    if not prod:
        prod = Product(
            source=item.source,
            external_id=item.external_id,
            title=item.title,
            url=item.url,
            img=item.img,
            img_hash=item.img_hash,
            brand=item.brand,
            category=item.category,
            finger=item.finger,
            geoid_created=item.geoid,
        )
        session.add(prod)
        await session.flush()
    else:
        if item.img_hash and not prod.img_hash:
            prod.img_hash = item.img_hash

    # Offer
    off = Offer(
        product_id=prod.id,
        price=item.price,
        price_old=item.price_old,
        price_final=item.price_final,
        seller=item.seller,
        shipping_days=item.shipping_days,
        promo_flags=item.promo_flags,
        price_in_cart=item.price_in_cart,
        subscription=item.subscription,
    )
    session.add(off)
    await session.flush()

    # History (append)
    hist = PriceHistory(
        product_id=prod.id,
        price_final=item.price_final,
        seller=item.seller
    )
    session.add(hist)
    return prod, off, hist

async def compute_features(session: AsyncSession, product_id: int) -> tuple[int | None, int | None, float | None]:
    """Расчёт средней цены за 30 дней, лучшей цены за 90 дней и тренда."""
    return await update_product_metrics(session, product_id)

async def process_preset(
    session: AsyncSession,
    render: RenderService,
    site: str,
    url: str,
    geoid: str | None,
    min_discount: int,
    min_score: int,
    score_weights: dict | None = None,
) -> list[dict]:
    raws = await fetch_site_list(render, site, url, geoid)
    normalized = [normalize(r) for r in raws]
    normalized = dedupe_offers(normalized)
    update_category_price_stats(normalized)

    results: list[dict] = []
    infos = []
    for n in normalized:
        infos.append(await upsert_offer(session, n))
    await session.commit()

    for (prod, off, _), n in zip(infos, normalized):
        avg30, best90, trend = await compute_features(session, prod.id)
        prod.avg_price_30d = avg30
        prod.min_price_90d = best90
        prod.trend_30d = trend

        abs_sav = (avg30 - (n.price_final or 0)) if avg30 and n.price_final else None
        disc = discount_pct(n.price_old or avg30, n.price_final)
        fake_msrp = is_fake_msrp(n.price_old, avg30)
        score = compute_score(disc, abs_sav, None, n.shipping_days, score_weights)

        off.discount_pct = disc
        off.abs_saving = abs_sav
        off.score = score
        off.fake_msrp = fake_msrp

        if disc is not None:
            n.discount_pct = disc
        if (disc is not None and disc >= min_discount) or score >= min_score:
            results.append({
                "title": n.title,
                "url": n.url,
                "price": n.price_final or n.price or 0,
                "discount_pct": disc,
                "score": score,
                "source": n.source,
                "img": n.img,
                "fake_msrp": fake_msrp,
            })

    await session.commit()
    results.sort(key=lambda x: x["score"], reverse=True)
    return results
