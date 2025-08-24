from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from ...schemas import OfferRaw

BASE = "https://www.ozon.ru"

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
    container = soup.select_one('[data-widget="searchResultsV2"]') or soup
    cards = container.select('a[href*="/product/"]')
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

        items.append(OfferRaw(
            source="ozon",
            title=title[:200] if title else "Товар Ozon",
            url=url,
            img=img,
            price=price,
            geoid=None
        ))
    return items

def external_id_from_url(url: str) -> str:
    # пример: https://www.ozon.ru/product/slug-123456789/ → берём последний числовой id
    path = urlparse(url).path
    m = re.search(r'(\d+)(?:/|$)', path)
    return m.group(1) if m else path.strip("/")
