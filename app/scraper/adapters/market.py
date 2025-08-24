from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from ...schemas import OfferRaw

BASE = "https://market.yandex.ru"

def parse_listing(html: str, geoid: str | None = None) -> list[OfferRaw]:
    """
    Упрощённый извлекатель из листинга Яндекс Маркета.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[OfferRaw] = []

    # карточки обычно в article[data-autotest-id='product-snippet']
    cards = soup.select("article[data-autotest-id='product-snippet']")
    for card in cards:
        link = card.select_one("a[href*='/product--']")
        if not link:
            continue
        href = link.get("href")
        url = urljoin(BASE, href)

        title_el = card.select_one("[data-baobab-name='title']") or link
        title = title_el.get_text(" ", strip=True) if title_el else "Товар Маркета"

        price_value = None
        price_el = card.select_one("[data-autotest-value]")
        if price_el and price_el.has_attr("data-autotest-value"):
            try:
                price_value = int(price_el["data-autotest-value"])
            except Exception:
                pass

        img_el = card.select_one("img")
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

def external_id_from_url(url: str) -> str:
    # market product URLs look like /product--slug/ID?...
    path = urlparse(url).path
    m = re.search(r'/product--[^/]+/(\d+)', path)
    return m.group(1) if m else path.strip("/")
