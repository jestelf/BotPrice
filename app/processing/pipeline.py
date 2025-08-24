from typing import Iterable
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

async def fetch_site_list(render: RenderService, site: str, url: str, geoid: str | None) -> list[OfferRaw]:
    if site == "ozon":
        html, _ = await render.fetch(url=url, wait_selector='[data-widget="searchResultsV2"]')
        return ozon_ad.parse_listing(html)
    elif site == "market":
        cookies = [{"name": "yandex_gid", "value": geoid or settings.DEFAULT_GEOID, "domain": ".yandex.ru", "path": "/"}]
        html, _ = await render.fetch(url=url, cookies=cookies, wait_selector="article[data-autotest-id='product-snippet']")
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
        promo_flags={"raw": True},
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

async def compute_features(session: AsyncSession, product_id: int) -> tuple[int | None, int | None]:
    """
    Возвращает (avg_30d, best_90d) на основе всей истории (упрощённо).
    Для прототипа посчитаем по всем точкам: среднее и минимум.
    """
    from sqlalchemy import func
    q = select(func.avg(PriceHistory.price_final), func.min(PriceHistory.price_final)).where(
        PriceHistory.product_id == product_id
    )
    res = await session.execute(q)
    avg_price, min_price = res.first() or (None, None)
    try:
        avg_price = int(avg_price) if avg_price is not None else None
    except Exception:
        pass
    try:
        min_price = int(min_price) if min_price is not None else None
    except Exception:
        pass
    return avg_price, min_price

async def process_preset(session: AsyncSession, render: RenderService, site: str, url: str, geoid: str | None, min_discount: int, min_score: int) -> list[dict]:
    raws = await fetch_site_list(render, site, url, geoid)
    normalized = [normalize(r) for r in raws]
    normalized = dedupe_by_finger(normalized)

    results: list[dict] = []
    # Upsert & score
    for n in normalized:
        await upsert_offer(session, n)
    await session.commit()

    # повторное чтение для вычисления фич
    for n in normalized:
        q = select(Product).where(Product.url == n.url)
        res = await session.execute(q)
        prod = res.scalar_one_or_none()
        if not prod:
            continue
        avg30, best90 = await compute_features(session, prod.id)
        disc = discount_pct(avg30 or n.price_old, n.price_final)
        score = compute_score(disc, (avg30 - (n.price_final or 0)) if avg30 and n.price_final else None, None, n.shipping_days)
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
                "img": n.img
            })
    # сортируем по score
    results.sort(key=lambda x: x["score"], reverse=True)
    return results
