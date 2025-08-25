from pydantic import BaseModel, AnyUrl, Field
from typing import Literal, Optional, Dict

Source = Literal["ozon", "market"]

class OfferRaw(BaseModel):
    source: Source
    title: str
    url: str
    img: Optional[AnyUrl] = None
    seller: Optional[str] = None
    price: Optional[int] = None
    price_old: Optional[int] = None
    shipping_days: Optional[int] = None
    shipping_included: bool = False
    promo_flags: Dict[str, int | bool] = Field(default_factory=dict)
    price_in_cart: bool = False
    subscription: bool = False
    geoid: Optional[str] = None

class OfferNormalized(BaseModel):
    source: Source
    external_id: str
    title: str
    url: str
    img: Optional[str] = None
    img_hash: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    seller: Optional[str] = None
    finger: str
    price: Optional[int] = None
    price_old: Optional[int] = None
    price_final: Optional[int] = None
    discount_pct: Optional[float] = None
    shipping_days: Optional[int] = None
    shipping_included: bool = False
    promo_flags: Dict[str, int | bool] = Field(default_factory=dict)
    price_in_cart: bool = False
    subscription: bool = False
    geoid: Optional[str] = None


class TaskPayload(BaseModel):
    site: Source
    url: str
    geoid: str
    category: str
    min_discount: int
    min_score: int
    notify: bool = False
    url_template: Optional[str] = None
    page: Optional[int] = None
    chat_id: Optional[int] = None
    weights: Optional[Dict[str, float]] = None
