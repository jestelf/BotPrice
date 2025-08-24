from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.schemas import OfferNormalized
from app.processing.dedupe import dedupe_offers


def make_offer(num: int, finger: str, img_hash: str, price: int) -> OfferNormalized:
    return OfferNormalized(
        source="ozon",
        external_id=str(num),
        title=f"t{num}",
        url=f"u{num}",
        img=None,
        img_hash=img_hash,
        brand=None,
        category=None,
        seller=None,
        finger=finger,
        price=price,
        price_old=None,
        price_final=price,
        discount_pct=None,
        shipping_days=None,
        promo_flags={},
        price_in_cart=False,
        subscription=False,
        geoid=None,
    )


def test_dedupe_by_img_hash():
    items = [make_offer(1, "f1", "i1", 100), make_offer(2, "f2", "i1", 90)]
    res = dedupe_offers(items)
    assert len(res) == 1
    assert res[0].external_id == "2"


def test_dedupe_by_finger():
    items = [make_offer(1, "f1", "i1", 100), make_offer(2, "f1", "i2", 90)]
    res = dedupe_offers(items)
    assert len(res) == 1
    assert res[0].external_id == "2"
