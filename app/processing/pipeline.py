from typing import Iterable
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse

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
from ..metrics import update_listing_stats, render_errors

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
        try:
            items = ozon_ad.parse_listing(html)
        except Exception as e:
            await render.save_snapshot(url, html, screenshot, prefix="schema")
            render_errors.labels(domain=domain).inc()
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
        try:
            items = market_ad.parse_listing(html, geoid=geoid)
        except Exception as e:
            await render.save_snapshot(url, html, screenshot, prefix="schema")
            render_errors.labels(domain=domain).inc()
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

async def compute_features(session: AsyncSession, product_id: int) -> tuple[dict, dict, float | None]:
    """
    Возвращает словари со статистикой за 30 и 90 дней и тренд за 30 дней.
    """
    from sqlalchemy import func

    now = datetime.utcnow()
    stats = {}
    for days in (30, 90):
        q = select(
            func.avg(PriceHistory.price_final),
            func.min(PriceHistory.price_final),
        ).where(
            PriceHistory.product_id == product_id,
            PriceHistory.ts >= now - timedelta(days=days)
        )
        res = await session.execute(q)
        avg_price, min_price = res.first() or (None, None)
        try:
            avg_price = int(avg_price) if avg_price is not None else None
        except Exception:
            avg_price = None
        try:
            min_price = int(min_price) if min_price is not None else None
        except Exception:
            min_price = None
        stats[days] = {"avg": avg_price, "min": min_price}

    # тренд: изменение цены за 30 дней в процентах
    q = select(PriceHistory.price_final, PriceHistory.ts).where(
        PriceHistory.product_id == product_id,
        PriceHistory.ts >= now - timedelta(days=30)
    ).order_by(PriceHistory.ts)
    res = await session.execute(q)
    rows = res.all()
    trend = None
    if len(rows) >= 2:
        first = rows[0][0]
        last = rows[-1][0]
        if first and last is not None and first > 0:
            trend = round((last - first) / first * 100, 2)

    return stats[30], stats[90], trend

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

    results: list[dict] = []
    infos = []
    for n in normalized:
        infos.append(await upsert_offer(session, n))
    await session.commit()

    for (prod, off, _), n in zip(infos, normalized):
        stats30, stats90, trend = await compute_features(session, prod.id)
        prod.avg_price_30d = stats30["avg"]
        prod.min_price_30d = stats30["min"]
        prod.avg_price_90d = stats90["avg"]
        prod.min_price_90d = stats90["min"]
        prod.trend_30d = trend

        abs_sav = (stats30["avg"] - (n.price_final or 0)) if stats30["avg"] and n.price_final else None
        disc = discount_pct(n.price_old or stats30["avg"], n.price_final)
        fake_msrp = is_fake_msrp(n.price_old, stats30["avg"], stats90["min"])
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
