from typing import Optional

def discount_pct(base: Optional[int], price_final: Optional[int]) -> Optional[float]:
    if base and price_final and base > 0:
        return round((base - price_final) / base * 100, 2)
    return None

def compute_score(disc_pct: Optional[float], abs_saving: Optional[int], seller_rating: Optional[float] = None, shipping_days: Optional[int] = None) -> float:
    dp = disc_pct or 0.0
    abs_s = (abs_saving or 0) / 100.0
    sr = (seller_rating or 0)  # 0..5 → можно масштабировать, но оставим как есть
    sd = -(shipping_days or 0)
    # веса (игрушечные, можно тюнинговать)
    return round(0.5*dp + 0.25*abs_s + 0.15*sr + 0.1*sd + 10, 2)
