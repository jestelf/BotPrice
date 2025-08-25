from dataclasses import dataclass
from typing import List, Dict

from app.scraper.adapters import market, ozon


@dataclass
class RenderContext:
    """Контекст рендера с регионом и куками."""

    geoid: str
    cookies: List[Dict[str, str]]


def create(geoid: str) -> RenderContext:
    """Создаёт контекст, устанавливая yandex_gid и регион Ozon."""
    cookies = market.region_cookies(geoid) + ozon.region_cookies(geoid)
    return RenderContext(geoid=geoid, cookies=cookies)
