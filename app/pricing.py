"""Расчёт финальной цены товаров."""
from typing import Optional, Dict, Tuple

from .config import settings


def compute_final_price(
    price: Optional[int],
    promo_flags: Dict[str, int | bool] | None = None,
    shipping_days: Optional[int] = None,
    shipping_included: bool = False,
    subscription: bool = False,
    price_in_cart: bool = False,
    with_raw: bool = False,
) -> Optional[int] | Tuple[Optional[int], Optional[int]]:
    """Высчитывает финальную цену с учётом купонов, доставки и подписки."""

    if price is None or price_in_cart:
        return (None, price) if with_raw else None

    coupon = 0
    if promo_flags and isinstance(promo_flags.get("instant_coupon"), int):
        coupon = int(promo_flags.get("instant_coupon", 0))

    total = price - coupon
    if shipping_days is not None and not subscription and not shipping_included:
        total += settings.SHIPPING_COST

    return (total, price) if with_raw else total

__all__ = ["compute_final_price"]
