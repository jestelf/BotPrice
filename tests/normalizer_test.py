import os
import sys
import pathlib
import hashlib

# ensure project root on path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("DATA_ENCRYPTION_KEY", "test")

from normalizer.core import normalize
from app.schemas import OfferRaw


def test_offer_raw_to_normalized():
    raw = OfferRaw.construct(
        source="ozon",
        title="  Apple   iPhone 14  ",
        url="https://www.ozon.ru/product/something-123456/",
        img="https://example.com/img.jpg",
        seller="  ozON  ",
        price="10 000 ₽",
        price_old="20 000 ₽",
        shipping_days=2,
        promo_flags={"instant_coupon": 1000},
        price_in_cart=False,
        subscription=False,
        geoid="213",
    )

    normalized = normalize(raw)

    assert normalized.title == "Apple iPhone 14"
    assert normalized.brand == "Apple"
    assert normalized.seller == "Ozon"
    assert normalized.price == 10000
    assert normalized.price_old == 20000
    assert normalized.price_final == 9199
    assert normalized.discount_pct == 50.0
    expected_finger = hashlib.md5("Apple iPhone 14Apple".encode("utf-8")).hexdigest()
    assert normalized.finger == expected_finger
    assert normalized.external_id == "123456"
