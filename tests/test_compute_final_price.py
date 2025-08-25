from pathlib import Path
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.pricing import compute_final_price
from app.config import settings


@pytest.mark.parametrize("shipping_cost", [199, 350])
def test_compute_final_price_with_coupon_and_shipping(monkeypatch, shipping_cost):
    monkeypatch.setattr(settings, "SHIPPING_COST", shipping_cost)
    total = compute_final_price(1000, {"instant_coupon": 100}, shipping_days=3)
    assert total == 1000 - 100 + shipping_cost


def test_compute_final_price_with_subscription(monkeypatch):
    monkeypatch.setattr(settings, "SHIPPING_COST", 250)
    total = compute_final_price(1000, shipping_days=5, subscription=True)
    assert total == 1000


def test_compute_final_price_price_in_cart():
    total = compute_final_price(1000, price_in_cart=True)
    assert total is None


def test_compute_final_price_return_raw(monkeypatch):
    monkeypatch.setattr(settings, "SHIPPING_COST", 300)
    final, raw = compute_final_price(
        2000, {"instant_coupon": 500}, shipping_days=2, with_raw=True
    )
    assert final == 2000 - 500 + 300
    assert raw == 2000


def test_compute_final_price_shipping_included(monkeypatch):
    monkeypatch.setattr(settings, "SHIPPING_COST", 250)
    total = compute_final_price(1000, shipping_days=3, shipping_included=True)
    assert total == 1000

