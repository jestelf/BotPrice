from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scraper.adapters.ozon import parse_listing as parse_ozon
from app.scraper.adapters.market import parse_listing as parse_market

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_parse_listing_ozon():
    html = load("ozon_listing.html")
    items = parse_ozon(html)
    assert len(items) == 2
    first = items[0]
    assert first.title.startswith("Товар A")
    assert first.price == 1234
    assert str(first.url).endswith("/product/123")
    assert str(first.img).endswith("/img1.jpg")


def test_parse_listing_market():
    html = load("market_listing.html")
    items = parse_market(html, geoid="213")
    assert len(items) == 2
    first = items[0]
    assert first.title == "Товар A"
    assert first.price == 1234
    assert str(first.url).endswith("/product--slug1/111")
    assert str(first.img).endswith("/img1.png")
    assert first.geoid == "213"
