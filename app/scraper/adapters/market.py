from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from ...schemas import OfferRaw
from ...pricing import compute_final_price as compute_final_price_common
from . import get_selectors, select_one, select_all

GEOID_TO_CITY = {
    "213": "Москва",
    "2": "Санкт-Петербург",
}

BASE = "https://market.yandex.ru"


def region_cookies(geoid: str) -> list[dict[str, str]]:
    """Возвращает куки для выбора региона."""
    return [
        {
            "name": "yandex_gid",
            "value": geoid,
            "domain": ".yandex.ru",
            "path": "/",
        }
    ]


def city_from_html(html: str) -> str | None:
    """Извлекает название города из HTML шапки сайта."""
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one("[data-autotest-id='region']") or soup.select_one(
        "[data-zone-name='region']"
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

def parse_listing(html: str, geoid: str | None = None) -> list[OfferRaw]:
    """Парсит листинг Яндекс Маркета."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[OfferRaw] = []

    selectors = get_selectors("market").get("listing", {})
    card_sel = selectors.get("card", {"css": "article[data-autotest-id='product-snippet']"})
    link_sel = selectors.get("link", {"css": "a[href*='/product--']"})
    title_sel = selectors.get("title", {"css": "[data-baobab-name='title']"})
    price_sel = selectors.get("price", {"css": "[data-autotest-value]"})
    image_sel = selectors.get("image", {"css": "img"})

    cards = select_all(soup, card_sel)
    for card in cards:
        link = select_one(card, link_sel)
        if not link:
            continue
        href = link.get("href")
        url = urljoin(BASE, href)

        title_el = select_one(card, title_sel) or link
        title = title_el.get_text(" ", strip=True) if title_el and hasattr(title_el, "get_text") else "Товар Маркета"

        price_value = None
        price_el = select_one(card, price_sel)
        if price_el and hasattr(price_el, "get") and price_el.get("data-autotest-value"):
            try:
                price_value = int(price_el["data-autotest-value"])
            except Exception:
                pass

        img_el = select_one(card, image_sel)
        img = urljoin(BASE, img_el.get("src")) if img_el and img_el.get("src") else None

        text_block = card.get_text(" ", strip=True).lower()

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
            source="market",
            title=title[:200],
            url=url,
            img=img,
            price=price_value,
            shipping_days=shipping_days,
            promo_flags=promo_flags,
            price_in_cart=price_in_cart,
            subscription=subscription,
            geoid=geoid
        ))
    return items


def parse_product(html: str, geoid: str | None = None) -> OfferRaw:
    """Парсит страницу товара Маркета."""
    soup = BeautifulSoup(html, "html.parser")
    selectors = get_selectors("market").get("product", {})

    link = soup.find("link", rel="canonical")
    url = urljoin(BASE, link.get("href")) if link and link.get("href") else BASE

    title_el = select_one(soup, selectors.get("title", {"css": "h1"}))
    title = (
        title_el.get_text(" ", strip=True)
        if title_el and hasattr(title_el, "get_text")
        else "Товар Маркета"
    )

    price_el = select_one(soup, selectors.get("price", {"css": "[data-auto='mainPrice']"}))
    price_text = price_el.get_text() if price_el and hasattr(price_el, "get_text") else str(price_el)
    price = _extract_price(price_text)

    img_el = select_one(soup, selectors.get("image", {"css": "img"}))
    img = urljoin(BASE, img_el.get("src")) if img_el and img_el.get("src") else None

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

    price_in_cart = "корзин" in text_block
    subscription = "подпис" in text_block

    offer = OfferRaw(
        source="market",
        title=title[:200],
        url=url,
        img=img,
        price=price,
        shipping_days=shipping_days,
        promo_flags=promo_flags,
        price_in_cart=price_in_cart,
        subscription=subscription,
        geoid=geoid,
    )
    return offer


def compute_final_price(offer: OfferRaw):
    """Считает финальную цену оффера, используя общий модуль."""
    return compute_final_price_common(
        offer.price,
        offer.promo_flags,
        offer.shipping_days,
        offer.subscription,
        offer.price_in_cart,
    )

def external_id_from_url(url: str) -> str:
    # market product URLs look like /product--slug/ID?...
    path = urlparse(url).path
    m = re.search(r'/product--[^/]+/(\d+)', path)
    return m.group(1) if m else path.strip("/")
