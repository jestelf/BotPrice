from collections import defaultdict

from prometheus_client import Counter, Gauge, Histogram


render_latency = Histogram(
    "render_latency_seconds", "Latency of page rendering", ["domain"]
)
render_errors = Counter(
    "render_errors_total", "Total render errors", ["domain"]
)
_listing_total = defaultdict(int)
_listing_empty = defaultdict(int)
listing_empty_share = Gauge(
    "listing_empty_share", "Share of empty listings", ["domain"]
)


def update_listing_stats(domain: str, empty: bool) -> None:
    """Обновляет счётчики и долю пустых листингов."""
    _listing_total[domain] += 1
    if empty:
        _listing_empty[domain] += 1
    share = _listing_empty[domain] / _listing_total[domain]
    listing_empty_share.labels(domain=domain).set(share)
