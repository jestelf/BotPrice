from pydantic import BaseModel, AnyUrl, Field
from typing import Literal, Optional, Set

Source = Literal["ozon", "market"]

class OfferRaw(BaseModel):
    source: Source
    title: str
    url: AnyUrl
    img: Optional[AnyUrl] = None
    seller: Optional[str] = None
    price: Optional[int] = None
    price_old: Optional[int] = None
    shipping_days: Optional[int] = None
    promo_flags: Set[str] = Field(default_factory=set)
    geoid: Optional[str] = None

class OfferNormalized(BaseModel):
    source: Source
    external_id: str
    title: str
    url: str
    img: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    seller: Optional[str] = None
    finger: str
    price: Optional[int] = None
    price_old: Optional[int] = None
    price_final: Optional[int] = None
    discount_pct: Optional[float] = None
    shipping_days: Optional[int] = None
    geoid: Optional[str] = None
