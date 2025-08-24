from typing import Optional

def discount_pct(base: Optional[int], price_final: Optional[int]) -> Optional[float]:
    if base and price_final and base > 0:
        return round((base - price_final) / base * 100, 2)
    return None

def compute_score(disc_pct: Optional[float], abs_saving: Optional[int], seller_rating: Optional[float] = None, shipping_days: Optional[int] = None) -> float:
    dp = disc_pct or 0.0
    abs_s = (abs_saving or 0) / 100.0
    sr = (seller_rating or 0) * 20  # 0..5 → 0..100
    sd = -(shipping_days or 0)
    # новая формула: веса 0.4/0.3/0.2/0.1 и базовый сдвиг 10
    return round(0.4*dp + 0.3*abs_s + 0.2*sr + 0.1*sd + 10, 2)
