from __future__ import annotations

from datetime import datetime, timedelta
from math import fsum
from typing import Sequence

from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Product, PriceHistory


def _calc_trend(rows: Sequence[tuple[int, datetime]]) -> float | None:
    if len(rows) < 2:
        return None
    base_ts = rows[0][1]
    xs = [((ts - base_ts).total_seconds() / 86400) for _, ts in rows]
    ys = [price for price, _ in rows]
    if len(ys) < 2:
        return None
    n = len(xs)
    sum_x = fsum(xs)
    sum_y = fsum(ys)
    sum_xx = fsum(x * x for x in xs)
    sum_xy = fsum(x * y for x, y in zip(xs, ys))
    denom = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return None
    slope = (n * sum_xy - sum_x * sum_y) / denom
    first = ys[0]
    if not first:
        return None
    trend = slope * 30 / first * 100
    return round(trend, 2)


async def update_product_metrics(session: AsyncSession, product_id: int) -> tuple[int | None, int | None, float | None]:
    now = datetime.utcnow()
    q30 = select(func.avg(PriceHistory.price_final)).where(
        PriceHistory.product_id == product_id,
        PriceHistory.ts >= now - timedelta(days=30),
    )
    avg_30 = await session.scalar(q30)
    avg_30 = int(avg_30) if avg_30 is not None else None

    q90 = select(func.min(PriceHistory.price_final)).where(
        PriceHistory.product_id == product_id,
        PriceHistory.ts >= now - timedelta(days=90),
    )
    best_90 = await session.scalar(q90)
    best_90 = int(best_90) if best_90 is not None else None

    qtrend = select(PriceHistory.price_final, PriceHistory.ts).where(
        PriceHistory.product_id == product_id,
        PriceHistory.ts >= now - timedelta(days=30),
        PriceHistory.price_final.isnot(None),
    ).order_by(PriceHistory.ts)
    rows = (await session.execute(qtrend)).all()
    trend = _calc_trend(rows)

    await session.execute(
        update(Product)
        .where(Product.id == product_id)
        .values(avg_price_30d=avg_30, min_price_90d=best_90, trend_30d=trend)
    )
    return avg_30, best_90, trend


async def refresh_all_products(session: AsyncSession) -> None:
    ids = (await session.execute(select(Product.id))).scalars().all()
    for pid in ids:
        await update_product_metrics(session, pid)
    await session.commit()
