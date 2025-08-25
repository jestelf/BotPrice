from typing import Optional


def is_fake_msrp(price_old: Optional[int], avg_price_30d: Optional[int]) -> bool:
    """Фейковый MSRP, если старая цена сильно выше средней за 30 дней."""
    if not price_old or not avg_price_30d:
        return False
    return price_old > avg_price_30d * 2
