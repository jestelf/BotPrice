import hashlib
import re
from typing import Optional

from app.schemas import OfferRaw, OfferNormalized
from app.scraper.adapters.ozon import external_id_from_url as ozon_id
from app.scraper.adapters.market import external_id_from_url as market_id
from app.pricing import compute_final_price


def _norm_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _clean_currency(value: Optional[str | int]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def _std_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return _norm_spaces(value).title()


def std_seller(value: Optional[str]) -> Optional[str]:
    return _std_name(value)


def std_brand(value: Optional[str]) -> Optional[str]:
    return _std_name(value)


def std_category(value: Optional[str]) -> Optional[str]:
    return _std_name(value)


def norm_title(title: str) -> str:
    return _norm_spaces(title)


def guess_brand(title: str) -> Optional[str]:
    known = [
        "lenovo",
        "asus",
        "acer",
        "hp",
        "huawei",
        "apple",
        "samsung",
        "xiaomi",
        "realme",
        "dell",
        "msi",
    ]
    tl = title.lower()
    for k in known:
        if k in tl:
            return k.capitalize()
    return None


def fingerprint(norm_title: str, brand: Optional[str] = None, model: Optional[str] = None) -> str:
    base = (norm_title or "") + (brand or "") + (model or "")
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def normalize(raw: OfferRaw) -> OfferNormalized:
    title = norm_title(raw.title)
    brand = std_brand(guess_brand(title))
    seller = std_seller(raw.seller)
    category = std_category(None)

    price = _clean_currency(raw.price)  # type: ignore[arg-type]
    price_old = _clean_currency(raw.price_old)  # type: ignore[arg-type]

    if raw.source == "ozon":
        external_id = ozon_id(str(raw.url))
    else:
        external_id = market_id(str(raw.url))

    price_final = compute_final_price(
        price,
        raw.promo_flags,
        raw.shipping_days,
        raw.subscription,
        raw.price_in_cart,
    )

    discount_pct = None
    if price_old and price and price_old > price:
        discount_pct = round((price_old - price) / price_old * 100, 2)

    finger = fingerprint(title, brand, None)

    return OfferNormalized(
        source=raw.source,
        external_id=external_id,
        title=title,
        url=str(raw.url),
        img=str(raw.img) if raw.img else None,
        img_hash=hashlib.md5(str(raw.img).encode("utf-8")).hexdigest() if raw.img else None,
        brand=brand,
        category=category,
        seller=seller,
        finger=finger,
        price=price,
        price_old=price_old,
        price_final=price_final,
        discount_pct=discount_pct,
        shipping_days=raw.shipping_days,
        promo_flags=raw.promo_flags,
        price_in_cart=raw.price_in_cart,
        subscription=raw.subscription,
        geoid=raw.geoid,
    )

__all__ = [
    "normalize",
    "norm_title",
    "guess_brand",
    "fingerprint",
    "std_seller",
    "std_brand",
    "std_category",
]
