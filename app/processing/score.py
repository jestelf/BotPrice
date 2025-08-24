from typing import Optional, Mapping

def discount_pct(base: Optional[int], price_final: Optional[int]) -> Optional[float]:
    if base and price_final and base > 0:
        return round((base - price_final) / base * 100, 2)
    return None

def compute_score(
    disc_pct: Optional[float],
    abs_saving: Optional[int],
    seller_rating: Optional[float] = None,
    shipping_days: Optional[int] = None,
    weights: Mapping[str, float] | None = None,
) -> float:
    dp = disc_pct or 0.0
    abs_s = (abs_saving or 0) / 100.0
    sr = (seller_rating or 0) * 20  # 0..5 → 0..100
    sd = -(shipping_days or 0)
    w = weights or {}
    wd = w.get("discount", 0.4)
    wa = w.get("abs", 0.3)
    ws = w.get("seller", 0.2)
    wh = w.get("shipping", 0.1)
    base = w.get("base", 10.0)
    return round(wd*dp + wa*abs_s + ws*sr + wh*sd + base, 2)
