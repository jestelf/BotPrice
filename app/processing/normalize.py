import hashlib
import re
from typing import Optional, Dict
from ..schemas import OfferRaw, OfferNormalized
from ..scraper.adapters.ozon import external_id_from_url as ozon_id
from ..scraper.adapters.market import external_id_from_url as market_id

def norm_title(t: str) -> str:
    return re.sub(r"\s+", " ", t).strip()

def fingerprint(title: str, brand: str | None = None, model: str | None = None) -> str:
    base = " ".join(filter(None, [title.lower(), brand, model]))
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def guess_brand(title: str) -> Optional[str]:
    # очень простая эвристика; в реальном проекте — словари/NER
    known = ["lenovo", "asus", "acer", "hp", "huawei", "apple", "samsung", "xiaomi", "realme", "dell", "msi"]
    tl = title.lower()
    for k in known:
        if k in tl:
            return k.capitalize()
    return None

FIXED_SHIPPING = 199


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
        total += FIXED_SHIPPING

    return total

def normalize(raw: OfferRaw) -> OfferNormalized:
    title = norm_title(raw.title)
    brand = guess_brand(title)
    if raw.source == "ozon":
        external_id = ozon_id(str(raw.url))
    else:
        external_id = market_id(str(raw.url))

    price_final = compute_final_price(
        raw.price,
        raw.promo_flags,
        raw.shipping_days,
        raw.subscription,
        raw.price_in_cart,
    )

    return OfferNormalized(
        source=raw.source,
        external_id=external_id,
        title=title,
        url=str(raw.url),
        img=str(raw.img) if raw.img else None,
        brand=brand,
        category=None,
        seller=raw.seller,
        finger=fingerprint(title, brand),
        price=raw.price,
        price_old=raw.price_old,
        price_final=price_final,
        discount_pct=None,  # посчитаем позже с историей
        shipping_days=raw.shipping_days,
        promo_flags=raw.promo_flags,
        price_in_cart=raw.price_in_cart,
        subscription=raw.subscription,
        geoid=raw.geoid,
    )
