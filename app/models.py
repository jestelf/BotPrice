from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Float, ForeignKey, DateTime, JSON, UniqueConstraint, Index, Text, Boolean
from datetime import datetime
from .db import Base

class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(16), index=True)  # "ozon" | "market"
    external_id: Mapped[str] = mapped_column(String(128), index=True)  # slug/id in URL
    title: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text, unique=True)
    img: Mapped[str | None] = mapped_column(Text, nullable=True)
    img_hash: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    brand: Mapped[str | None] = mapped_column(String(64))
    category: Mapped[str | None] = mapped_column(String(64))
    finger: Mapped[str] = mapped_column(String(32), index=True)  # md5 fingerprint
    geoid_created: Mapped[str | None] = mapped_column(String(16))

    avg_price_30d: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_price_30d: Mapped[int | None] = mapped_column(Integer, nullable=True)
    avg_price_90d: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_price_90d: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trend_30d: Mapped[float | None] = mapped_column(Float, nullable=True)

    offers: Mapped[list["Offer"]] = relationship(back_populates="product", cascade="all, delete-orphan")
    history: Mapped[list["PriceHistory"]] = relationship(back_populates="product", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_source_external"),
    )

class Offer(Base):
    __tablename__ = "offers"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"))
    price: Mapped[int | None] = mapped_column(Integer)
    price_old: Mapped[int | None] = mapped_column(Integer)
    price_final: Mapped[int | None] = mapped_column(Integer, index=True)
    seller: Mapped[str | None] = mapped_column(String(128))
    shipping_days: Mapped[int | None] = mapped_column(Integer)
    promo_flags: Mapped[dict | None] = mapped_column(JSON)
    price_in_cart: Mapped[bool | None] = mapped_column(Boolean)
    subscription: Mapped[bool | None] = mapped_column(Boolean)
    scraped_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    discount_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    abs_saving: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    fake_msrp: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    product: Mapped["Product"] = relationship(back_populates="offers")
    __table_args__ = (
        Index("ix_offers_product_time", "product_id", "scraped_at"),
    )

class PriceHistory(Base):
    __tablename__ = "price_history"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    price_final: Mapped[int | None] = mapped_column(Integer)
    seller: Mapped[str | None] = mapped_column(String(128))

    product: Mapped["Product"] = relationship(back_populates="history")

class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    type: Mapped[str] = mapped_column(String(32))  # "price_drop", "back_in_stock", etc.
    payload_json: Mapped[dict | None] = mapped_column(JSON)

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    geoid: Mapped[str] = mapped_column(String(16), default="213")
    min_discount: Mapped[int] = mapped_column(Integer, default=25)
    min_score: Mapped[int] = mapped_column(Integer, default=70)
    filters_json: Mapped[dict | None] = mapped_column(JSON)
    score_weights_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    schedule_cron: Mapped[str | None] = mapped_column(String(64))  # e.g., "0 9,19 * * *"

class Favorite(Base):
    __tablename__ = "favorites"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    geoid: Mapped[str | None] = mapped_column(String(16), nullable=True)
    min_discount: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_cron: Mapped[str | None] = mapped_column(String(64), nullable=True)
