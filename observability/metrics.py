from prometheus_client import Counter, Histogram

render_latency = Histogram(
    "render_latency_seconds", "Latency of page rendering", ["domain"]
)
render_errors = Counter(
    "render_errors_total", "Total render errors", ["domain"]
)
parse_latency = Histogram(
    "parse_latency_seconds", "Latency of HTML parsing", ["domain"]
)
parse_errors = Counter(
    "parse_errors_total", "Total parse errors", ["domain"]
)
