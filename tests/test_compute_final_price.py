from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.processing.normalize import compute_final_price, FIXED_SHIPPING


def test_compute_final_price_with_coupon_and_shipping():
    total = compute_final_price(1000, {"instant_coupon": 100}, shipping_days=3)
    assert total == 1000 - 100 + FIXED_SHIPPING


def test_compute_final_price_with_subscription():
    total = compute_final_price(1000, shipping_days=5, subscription=True)
    assert total == 1000


def test_compute_final_price_price_in_cart():
    total = compute_final_price(1000, price_in_cart=True)
    assert total is None
