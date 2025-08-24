import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import types
import importlib

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import select, func

# Ensure required environment variables for settings
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.models import Base, Product, PriceHistory
from app.schemas import OfferNormalized
from app.processing.detectors import is_fake_msrp


def load_pipeline(monkeypatch):
    """Import pipeline module with heavy deps stubbed."""
    monkeypatch.setitem(sys.modules, "app.scraper.render", types.SimpleNamespace(RenderService=object))

    adapters_pkg = types.ModuleType("app.scraper.adapters")
    adapters_pkg.__path__ = []
    ozon_stub = types.ModuleType("ozon")
    ozon_stub.region_cookies = lambda geoid: []
    ozon_stub.parse_listing = lambda html, **k: []
    ozon_stub.external_id_from_url = lambda url: "id"
    market_stub = types.ModuleType("market")
    market_stub.region_cookies = lambda geoid: []
    market_stub.parse_listing = lambda html, geoid=None: []
    market_stub.external_id_from_url = lambda url: "id"

    monkeypatch.setitem(sys.modules, "app.scraper.adapters", adapters_pkg)
    monkeypatch.setitem(sys.modules, "app.scraper.adapters.ozon", ozon_stub)
    monkeypatch.setitem(sys.modules, "app.scraper.adapters.market", market_stub)
    monkeypatch.setitem(
        sys.modules,
        "app.metrics",
        types.SimpleNamespace(
            update_listing_stats=lambda *a, **k: None,
            update_category_price_stats=lambda *a, **k: None,
            render_errors=types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(inc=lambda: None)),
        ),
    )

    pipeline = importlib.import_module("app.processing.pipeline")
    return pipeline.compute_features, pipeline.upsert_offer


@pytest.mark.asyncio
async def test_price_history_stats_and_trend(monkeypatch):
    compute_features, _ = load_pipeline(monkeypatch)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        prod = Product(source="ozon", external_id="1", title="t", url="u", finger="f")
        session.add(prod)
        await session.flush()

        now = datetime.utcnow()
        session.add_all([
            PriceHistory(product_id=prod.id, price_final=200, ts=now - timedelta(days=40)),
            PriceHistory(product_id=prod.id, price_final=100, ts=now - timedelta(days=20)),
            PriceHistory(product_id=prod.id, price_final=80, ts=now - timedelta(days=10)),
            PriceHistory(product_id=prod.id, price_final=120, ts=now - timedelta(days=1)),
        ])
        await session.commit()

        stats30, stats90, trend = await compute_features(session, prod.id)

        assert stats30 == {"avg": 100, "min": 80}
        assert stats90 == {"avg": 125, "min": 80}
        assert trend == 20.0


@pytest.mark.asyncio
async def test_upsert_adds_price_history(monkeypatch):
    _, upsert_offer = load_pipeline(monkeypatch)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        item = OfferNormalized(
            source="ozon",
            external_id="10",
            title="t10",
            url="u10",
            img=None,
            img_hash=None,
            brand=None,
            category=None,
            seller=None,
            finger="f10",
            price=150,
            price_old=None,
            price_final=150,
            discount_pct=None,
            shipping_days=None,
            promo_flags={},
            price_in_cart=False,
            subscription=False,
            geoid=None,
        )
        prod, offer, hist = await upsert_offer(session, item)
        await session.commit()

        q = select(func.count(PriceHistory.id))
        count = await session.scalar(q)
        assert count == 1
        entry = await session.scalar(select(PriceHistory))
        assert entry.price_final == 150
        assert entry.product_id == prod.id


def test_is_fake_msrp():
    assert is_fake_msrp(200, 100, 80) is True
    assert is_fake_msrp(110, 100, 80) is False
    assert is_fake_msrp(130, None, 80) is True
    assert is_fake_msrp(None, 100, 80) is False
