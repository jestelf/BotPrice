import random
from typing import Iterable
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from ..scraper.render import RenderService
from ..scraper.adapters import ozon as ozon_ad, market as market_ad
from ..schemas import OfferRaw, OfferNormalized
from ..processing.normalize import normalize
from ..processing.score import discount_pct, compute_score
from ..models import Product, Offer, PriceHistory
from ..config import settings

def dedupe_by_finger(items: Iterable[OfferNormalized]) -> list[OfferNormalized]:
    best: dict[str, OfferNormalized] = {}
    for it in items:
        key = it.finger
        if key not in best:
            best[key] = it
        else:
            prev = best[key]
            # выбираем с меньшей ценой
            if (it.price_final or 10**12) < (prev.price_final or 10**12):
                best[key] = it
    return list(best.values())

async def fetch_site_list(
    render: RenderService, site: str, url: str, geoid: str | None
) -> list[OfferRaw]:
    ttl = random.randint(30, 180)
    geoid_actual = geoid or settings.DEFAULT_GEOID
    if site == "ozon":
        cookies = ozon_ad.region_cookies(geoid_actual)
        html, _ = await render.fetch(
            url=url,
            cookies=cookies,
            wait_selector='[data-widget="searchResultsV2"]',
            region_hint=geoid,
            cache_ttl=ttl,
        )
        if not ozon_ad.ensure_region(html, geoid_actual):
            raise ValueError("Не удалось выбрать регион")
        return ozon_ad.parse_listing(html)
    elif site == "market":
        cookies = market_ad.region_cookies(geoid_actual)
        html, _ = await render.fetch(
            url=url,
            cookies=cookies,
            wait_selector="article[data-autotest-id='product-snippet']",
            region_hint=geoid,
            cache_ttl=ttl,
        )
        if not market_ad.ensure_region(html, geoid_actual):
            raise ValueError("Не удалось выбрать регион")
        return market_ad.parse_listing(html, geoid=geoid)
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
            brand=item.brand,
            category=item.category,
            finger=item.finger,
            geoid_created=item.geoid,
        )
        session.add(prod)
        await session.flush()

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

async def compute_features(session: AsyncSession, product_id: int) -> tuple[dict, dict]:
    """
    Возвращает словари со статистикой за 30 и 90 дней: {"avg": int|None, "min": int|None}.
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
    return stats[30], stats[90]

async def process_preset(session: AsyncSession, render: RenderService, site: str, url: str, geoid: str | None, min_discount: int, min_score: int) -> list[dict]:
    raws = await fetch_site_list(render, site, url, geoid)
    normalized = [normalize(r) for r in raws]
    normalized = dedupe_by_finger(normalized)

    results: list[dict] = []
    infos = []
    for n in normalized:
        infos.append(await upsert_offer(session, n))
    await session.commit()

    for (prod, off, _), n in zip(infos, normalized):
        stats30, stats90 = await compute_features(session, prod.id)
        prod.avg_price_30d = stats30["avg"]
        prod.min_price_30d = stats30["min"]
        prod.avg_price_90d = stats90["avg"]
        prod.min_price_90d = stats90["min"]

        abs_sav = (stats30["avg"] - (n.price_final or 0)) if stats30["avg"] and n.price_final else None
        disc = discount_pct(n.price_old or stats30["avg"], n.price_final)
        fake_msrp = False
        if n.price_old and stats30["avg"]:
            fake_msrp = n.price_old > stats30["avg"] * 1.5
        score = compute_score(disc, abs_sav, None, n.shipping_days)

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
