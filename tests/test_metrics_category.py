import sys
from pathlib import Path

# Ensure required environment variables for settings
import os
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.schemas import OfferNormalized
import app.metrics as metrics


def make_item(price, category="cat"):
    return OfferNormalized(
        source="ozon",
        external_id=str(price or "x"),
        title="t",
        url=f"https://ex.com/{price}",
        finger=str(price or "x"),
        price=price,
        category=category,
    )


def test_category_metrics_and_trigger(monkeypatch):
    messages = []
    monkeypatch.setattr(metrics, "notify_monitoring", lambda m: messages.append(m))

    metrics._category_counts.clear()
    metrics.category_avg_price.labels(category="cat").set(0)
    metrics.category_no_price_share.labels(category="cat").set(0)

    items = [
        make_item(100),
        make_item(200),
        make_item(300),
        make_item(None),
    ]
    metrics.update_category_price_stats(items)

    assert metrics.category_avg_price.labels(category="cat")._value.get() == 200
    assert metrics.category_no_price_share.labels(category="cat")._value.get() == 0.25

    items2 = [make_item(1000)]
    metrics.update_category_price_stats(items2)
    assert len(messages) == 2
    assert any("количества" in m for m in messages)
    assert any("средней цены" in m for m in messages)
