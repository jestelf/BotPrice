"""Расчёт финальной цены товаров."""
from typing import Optional, Dict

from .config import settings


def compute_final_price(
    price: Optional[int],
    promo_flags: Dict[str, int | bool] | None = None,
    shipping_days: Optional[int] = None,
    subscription: bool = False,
    price_in_cart: bool = False,
) -> Optional[int]:
    """Высчитывает финальную цену с учётом купонов, доставки и подписки."""

    if price is None or price_in_cart:
        return None

    coupon = 0
    if promo_flags and isinstance(promo_flags.get("instant_coupon"), int):
        coupon = int(promo_flags.get("instant_coupon", 0))

    total = price - coupon
    if shipping_days is not None and not subscription:
        total += settings.SHIPPING_COST

    return total

__all__ = ["compute_final_price"]
