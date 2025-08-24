from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from ...schemas import OfferRaw
from . import get_selectors

GEOID_TO_CITY = {
    "213": "Москва",
    "2": "Санкт-Петербург",
}

BASE = "https://www.ozon.ru"


def region_cookies(geoid: str) -> list[dict[str, str]]:
    """Возвращает куки для выбора региона."""
    return [
        {
            "name": "region",
            "value": geoid,
            "domain": ".ozon.ru",
            "path": "/",
        }
    ]


def city_from_html(html: str) -> str | None:
    """Извлекает название города из HTML шапки сайта."""
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one("[data-widget='headerLocation']") or soup.select_one(
        "[data-widget='regionSelect']"
    )
    return el.get_text(strip=True) if el else None


def ensure_region(html: str, geoid: str) -> bool:
    """Проверяет, что отображаемый город соответствует geoid."""
    expected = GEOID_TO_CITY.get(geoid)
    if not expected:
        return True
    city = city_from_html(html)
    return city == expected

def _extract_price(text: str | None):
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else None

def parse_listing(html: str) -> list[OfferRaw]:
    """
    Упрощённый извлекатель из листинга Ozon.
    Селекторы подвержены изменениям — предусмотрены fallback-ы.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[OfferRaw] = []

    # Ozon листинг часто живёт в data-widget="searchResultsV2"
    selectors = get_selectors("ozon")
    container_sel = selectors.get("container", '[data-widget="searchResultsV2"]')
    card_sel = selectors.get("card", 'a[href*="/product/"]')

    container = soup.select_one(container_sel) or soup
    cards = container.select(card_sel)
    seen = set()
    for a in cards:
        href = a.get("href")
        if not href or "/product/" not in href:
            continue
        url = urljoin(BASE, href)
        if url in seen:
            continue
        seen.add(url)

        title = a.get_text(" ", strip=True)
        if not title:
            # иногда заголовок рядом
            title = (a.find_next("span") or {}).get_text(strip=True) if hasattr(a.find_next("span"), "get_text") else ""

        # цена — пытаемся найти ближайший контейнер с цифрами
        price_el = a.find_next(lambda tag: tag.name in ("span", "div") and re.search(r"\d[\d\s]*₽", tag.get_text()))
        price = _extract_price(price_el.get_text() if price_el else "")

        img = None
        img_el = a.find("img")
        if img_el and img_el.get("src"):
            img = urljoin(BASE, img_el.get("src"))

        # простые эвристики для купонов и доставки
        text_block = a.get_text(" ", strip=True).lower()

        promo_flags: dict[str, int | bool] = {}
        m_coupon = re.search(r"купон.*?(\d+)", text_block)
        if m_coupon:
            try:
                promo_flags["instant_coupon"] = int(m_coupon.group(1))
            except Exception:
                pass

        shipping_days = None
        m_ship = re.search(r"(\d+)[^\d]{0,5}дн", text_block)
        if m_ship:
            try:
                shipping_days = int(m_ship.group(1))
            except Exception:
                pass

        price_in_cart = "корзин" in text_block
        subscription = "подпис" in text_block

        items.append(OfferRaw(
            source="ozon",
            title=title[:200] if title else "Товар Ozon",
            url=url,
            img=img,
            price=price,
            shipping_days=shipping_days,
            promo_flags=promo_flags,
            price_in_cart=price_in_cart,
            subscription=subscription,
            geoid=None
        ))
    return items

def external_id_from_url(url: str) -> str:
    # пример: https://www.ozon.ru/product/slug-123456789/ → берём последний числовой id
    path = urlparse(url).path
    m = re.search(r'(\d+)(?:/|$)', path)
    return m.group(1) if m else path.strip("/")
