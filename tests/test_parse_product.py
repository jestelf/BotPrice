from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scraper.adapters.ozon import parse_product as parse_ozon
from app.scraper.adapters.market import parse_product as parse_market

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_parse_product_ozon():
    html = load("ozon_product.html")
    offer = parse_ozon(html)
    assert offer.title == "Товар A"
    assert offer.price == 1234
    assert str(offer.url).endswith("/product/123")
    assert offer.shipping_days == 3
    assert offer.subscription is True
    assert offer.promo_flags.get("instant_coupon") == 100


def test_parse_product_market():
    html = load("market_product.html")
    offer = parse_market(html, geoid="213")
    assert offer.title == "Товар A"
    assert offer.price == 1234
    assert str(offer.url).endswith("/product--slug1/111")
    assert offer.shipping_days == 5
    assert offer.subscription is True
    assert offer.promo_flags.get("instant_coupon") == 200
    assert offer.geoid == "213"
