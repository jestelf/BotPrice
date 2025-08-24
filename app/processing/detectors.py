from typing import Optional


def is_fake_msrp(price_old: Optional[int], avg_price_30d: Optional[int], best_price_90d: Optional[int]) -> bool:
    """Простая эвристика выявления завышенного MSRP."""
    if not price_old:
        return False
    baselines = []
    if avg_price_30d:
        baselines.append(avg_price_30d)
    if best_price_90d:
        baselines.append(best_price_90d)
    if not baselines:
        return False
    baseline = min(baselines)
    return price_old > baseline * 1.5
