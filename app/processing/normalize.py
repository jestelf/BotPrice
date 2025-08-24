import hashlib
import re
from typing import Optional
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

def compute_final_price(price: Optional[int], promo_flags: set[str] | None = None, shipping_days: Optional[int] = None) -> Optional[int]:
    # базово возвращаем price — сюда можно добавить минус купон и т.п., если извлекли
    return price

def normalize(raw: OfferRaw) -> OfferNormalized:
    title = norm_title(raw.title)
    brand = guess_brand(title)
    if raw.source == "ozon":
        external_id = ozon_id(str(raw.url))
    else:
        external_id = market_id(str(raw.url))

    price_final = compute_final_price(raw.price, raw.promo_flags, raw.shipping_days)

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
        geoid=raw.geoid,
    )
