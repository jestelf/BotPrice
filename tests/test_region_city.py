from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scraper.adapters import ozon, market

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_market_region_parsing():
    html_msk = load("market_region_msk.html")
    html_spb = load("market_region_spb.html")
    assert market.city_from_html(html_msk) == "Москва"
    assert market.city_from_html(html_spb) == "Санкт-Петербург"
    assert market.ensure_region(html_msk, "213")
    assert market.ensure_region(html_spb, "2")


def test_ozon_region_parsing():
    html_msk = load("ozon_region_msk.html")
    html_spb = load("ozon_region_spb.html")
    assert ozon.city_from_html(html_msk) == "Москва"
    assert ozon.city_from_html(html_spb) == "Санкт-Петербург"
    assert ozon.ensure_region(html_msk, "213")
    assert ozon.ensure_region(html_spb, "2")
