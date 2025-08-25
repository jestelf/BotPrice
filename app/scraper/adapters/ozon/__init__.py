import json
import os
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from ....schemas import OfferRaw
from ....pricing import compute_final_price as compute_final_price_common
from .. import get_selectors, select_one, select_all
from ... import logger

GEOID_TO_CITY = {
    "213": "Москва",
    "2": "Санкт-Петербург",
}
_extra = os.getenv("OZON_GEOID_TO_CITY")
if _extra:
    try:
        GEOID_TO_CITY.update(json.loads(_extra))
    except Exception:
        logger.warning("Не удалось разобрать OZON_GEOID_TO_CITY")

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
    if el:
        return el.get_text(strip=True)
    m = re.search(r"Товары для города\s+([\w\-\s]+)", html)
    if m:
        return m.group(1).strip()
    return None


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
    """Парсит листинг Ozon."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[OfferRaw] = []

    selectors = get_selectors("ozon").get("listing", {})
    container_sel = selectors.get("container", {"css": '[data-widget="searchResultsV2"]'})
    card_sel = selectors.get("card", {"css": 'a[href*="/product/"]'})
    price_sel = selectors.get("price")
    image_sel = selectors.get("image", {"css": "img"})

    container = select_one(soup, container_sel) or soup
    cards = select_all(container, card_sel)
    seen = set()
    for a in cards:
        href = a.get("href")
        if not href or "/product/" not in href:
            logger.warning("пропуск карточки: отсутствует ссылка")
            continue
        url = urljoin(BASE, href)
        if url in seen:
            continue
        seen.add(url)

        title = a.get_text(" ", strip=True)
        if not title:
            # иногда заголовок рядом
            title = (
                (a.find_next("span") or {})
                .get_text(strip=True)
                if hasattr(a.find_next("span"), "get_text")
                else ""
            )

        price_el = select_one(a, price_sel) if price_sel else None
        price = None
        if price_el is not None:
            text = price_el.get_text() if hasattr(price_el, "get_text") else str(price_el)
            price = _extract_price(text)
        if price is None:
            # fallback к поиску по тексту
            price_el = a.find_next(
                lambda tag: tag.name in ("span", "div")
                and re.search(r"\d[\d\s]*₽", tag.get_text())
            )
            price = _extract_price(price_el.get_text() if price_el else "")
        if price is None:
            logger.warning("пропуск карточки %s: отсутствует цена", url)
            continue

        img = None
        img_el = select_one(a, image_sel)
        if img_el is not None:
            if hasattr(img_el, "get") and img_el.get("src"):
                img = urljoin(BASE, img_el.get("src"))
            else:
                img = urljoin(BASE, str(img_el))

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

        shipping_included = "бесп" in text_block
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
            shipping_included=shipping_included,
            price_in_cart=price_in_cart,
            subscription=subscription,
            geoid=None
        ))
    return items


def parse_product(html: str) -> OfferRaw:
    """Парсит страницу товара Ozon."""
    soup = BeautifulSoup(html, "html.parser")
    selectors = get_selectors("ozon").get("product", {})

    link = soup.find("link", rel="canonical")
    url = urljoin(BASE, link.get("href")) if link and link.get("href") else BASE

    title_el = select_one(soup, selectors.get("title", {"css": "h1"}))
    title = (
        title_el.get_text(" ", strip=True)
        if title_el and hasattr(title_el, "get_text")
        else "Товар Ozon"
    )

    price_el = select_one(soup, selectors.get("price", {"css": "[data-widget='webPrice']"}))
    price_text = price_el.get_text() if price_el and hasattr(price_el, "get_text") else str(price_el)
    price = _extract_price(price_text)

    img_el = select_one(soup, selectors.get("image", {"css": "img"}))
    if img_el is not None:
        if hasattr(img_el, "get") and img_el.get("src"):
            img = urljoin(BASE, img_el.get("src"))
        else:
            img = urljoin(BASE, str(img_el))
    else:
        img = None

    text_block = soup.get_text(" ", strip=True).lower()
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

    shipping_included = "бесп" in text_block
    price_in_cart = "корзин" in text_block
    subscription = "подпис" in text_block

    offer = OfferRaw(
        source="ozon",
        title=title[:200],
        url=url,
        img=img,
        price=price,
        shipping_days=shipping_days,
        promo_flags=promo_flags,
        shipping_included=shipping_included,
        price_in_cart=price_in_cart,
        subscription=subscription,
        geoid=None,
    )
    return offer


def compute_final_price(offer: OfferRaw):
    """Считает финальную цену оффера, используя общий модуль."""
    return compute_final_price_common(
        offer.price,
        offer.promo_flags,
        offer.shipping_days,
        offer.shipping_included,
        offer.subscription,
        offer.price_in_cart,
    )

def external_id_from_url(url: str) -> str:
    # пример: https://www.ozon.ru/product/slug-123456789/ → берём последний числовой id
    path = urlparse(url).path
    m = re.search(r'(\d+)(?:/|$)', path)
    return m.group(1) if m else path.strip("/")
