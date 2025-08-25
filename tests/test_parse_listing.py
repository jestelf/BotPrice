from pathlib import Path
import sys
import logging

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


def test_parse_listing_ozon_json():
    html = (
        "<div data-widget='searchResultsV2'>"
        "<a href='/product/123'><span>Товар A</span>"
        "<script>{\"price\":{\"current\":1234},\"image\":{\"url\":\"/img1.jpg\"}}</script>"
        "</a></div>"
    )
    items = parse_ozon(html)
    assert len(items) == 1
    first = items[0]
    assert first.price == 1234
    assert str(first.img).endswith("/img1.jpg")


def test_parse_listing_market_json():
    html = (
        "<article data-autotest-id='product-snippet'>"
        "<a href='/product--slug1/111'></a>"
        "<div data-baobab-name='title'>Товар A</div>"
        "<script>{\"price\":{\"value\":1234},\"image\":{\"url\":\"/img1.png\"}}</script>"
        "</article>"
    )
    items = parse_market(html, geoid="213")
    assert len(items) == 1
    first = items[0]
    assert first.price == 1234
    assert str(first.img).endswith("/img1.png")


def test_parse_listing_ozon_missing_link(monkeypatch, caplog):
    html = "<div data-widget='searchResultsV2'><a>Товар</a></div>"

    def fake_get_selectors(name):
        return {"listing": {"container": {"css": "[data-widget='searchResultsV2']"}, "card": {"css": "a"}}}

    monkeypatch.setattr("app.scraper.adapters.ozon.get_selectors", fake_get_selectors)
    with caplog.at_level(logging.WARNING):
        items = parse_ozon(html)
    assert items == []
    assert "отсутствует ссылка" in caplog.text


def test_parse_listing_ozon_missing_price(caplog):
    html = "<div data-widget='searchResultsV2'><a href='/product/123'>Товар</a></div>"
    with caplog.at_level(logging.WARNING):
        items = parse_ozon(html)
    assert items == []
    assert "отсутствует цена" in caplog.text


def test_parse_listing_market_missing_link(caplog):
    html = "<article data-autotest-id='product-snippet'><span>Товар</span></article>"
    with caplog.at_level(logging.WARNING):
        items = parse_market(html, geoid="213")
    assert items == []
    assert "отсутствует ссылка" in caplog.text


def test_parse_listing_market_missing_price(caplog):
    html = "<article data-autotest-id='product-snippet'><a href='/product--slug1/111'>Товар</a></article>"
    with caplog.at_level(logging.WARNING):
        items = parse_market(html, geoid="213")
    assert items == []
    assert "отсутствует цена" in caplog.text
