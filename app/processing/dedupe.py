from typing import Iterable
from ..schemas import OfferNormalized


def dedupe_offers(items: Iterable[OfferNormalized]) -> list[OfferNormalized]:
    """Удаляет дубликаты по finger или img_hash, оставляя предложение с минимальной ценой."""
    by_finger: dict[str, OfferNormalized] = {}
    by_img: dict[str, OfferNormalized] = {}
    result: list[OfferNormalized] = []
    for it in items:
        existing = by_finger.get(it.finger)
        if not existing and it.img_hash:
            existing = by_img.get(it.img_hash)
        if existing:
            prev = existing
            if (it.price_final or 10**12) < (prev.price_final or 10**12):
                idx = result.index(prev)
                result[idx] = it
                by_finger[it.finger] = it
                if it.img_hash:
                    by_img[it.img_hash] = it
            # если текущий хуже, ничего не делаем
        else:
            result.append(it)
            by_finger[it.finger] = it
            if it.img_hash:
                by_img[it.img_hash] = it
    return result
