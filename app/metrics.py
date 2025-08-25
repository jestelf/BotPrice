from collections import defaultdict
from typing import Iterable

from prometheus_client import Counter, Gauge, Histogram
from statistics import median, quantiles

from .notifier.monitoring import notify_monitoring
from .schemas import OfferNormalized


dlq_tasks_total = Counter(
    "dlq_tasks_total", "Total tasks processed from DLQ"
)
dlq_backlog = Gauge(
    "dlq_backlog", "Current number of tasks in DLQ"
)
_listing_total = defaultdict(int)
_listing_empty = defaultdict(int)
listing_empty_share = Gauge(
    "listing_empty_share", "Share of empty listings", ["domain"]
)

# Бюджет и пропуски задач
budget_exceeded = Counter(
    "budget_exceeded_total", "Total budget exceed events", ["type"]
)
tasks_skipped = Counter(
    "tasks_skipped_total", "Total skipped tasks", ["reason"]
)

# Метрики по категориям
category_avg_price = Gauge(
    "category_avg_price", "Average price per category", ["category"]
)
category_no_price_share = Gauge(
    "category_no_price_share", "Share of items without price", ["category"]
)
category_price_p50 = Gauge(
    "category_price_p50", "Median price per category", ["category"]
)
category_price_p90 = Gauge(
    "category_price_p90", "P90 price per category", ["category"]
)
_category_counts = defaultdict(int)
_category_avg = defaultdict(float)


def update_listing_stats(domain: str, empty: bool) -> None:
    """Обновляет счётчики и долю пустых листингов."""
    _listing_total[domain] += 1
    if empty:
        _listing_empty[domain] += 1
    share = _listing_empty[domain] / _listing_total[domain]
    listing_empty_share.labels(domain=domain).set(share)


def update_category_price_stats(items: Iterable[OfferNormalized]) -> None:
    """Обновляет среднюю цену и долю карточек без цены по категориям.

    Одновременно отслеживает резкие изменения количества карточек и
    отправляет уведомление в канал мониторинга.
    """
    stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"total": 0, "sum": 0.0, "with_price": 0, "prices": []}
    )
    for it in items:
        cat = it.category or "unknown"
        s = stats[cat]
        s["total"] += 1
        if it.price is not None:
            s["sum"] += it.price
            s["with_price"] += 1
            s["prices"].append(it.price)

    for cat, s in stats.items():
        total = s["total"]
        with_price = s["with_price"]
        avg = s["sum"] / with_price if with_price else 0
        category_avg_price.labels(category=cat).set(avg)
        no_price_share = (total - with_price) / total if total else 0
        category_no_price_share.labels(category=cat).set(no_price_share)

        if s["prices"]:
            category_price_p50.labels(category=cat).set(median(s["prices"]))
            try:
                p90 = quantiles(s["prices"], n=100)[89]
            except Exception:
                p90 = s["prices"][0]
            category_price_p90.labels(category=cat).set(p90)

        prev = _category_counts[cat]
        if prev and (total < prev * 0.5 or total > prev * 2):
            notify_monitoring(
                f"Аномальное изменение количества карточек в категории {cat}: {prev} → {total}"
            )
        _category_counts[cat] = total

        prev_avg = _category_avg[cat]
        if prev_avg and (avg < prev_avg * 0.5 or avg > prev_avg * 2):
            notify_monitoring(
                f"Аномальное изменение средней цены в категории {cat}: {prev_avg:.2f} → {avg:.2f}"
            )
        _category_avg[cat] = avg
