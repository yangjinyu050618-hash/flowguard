"""Prometheus and JSON metrics exporter."""

import json
from flowguard.metrics.collector import MetricsCollector


def export_json(collector: MetricsCollector) -> str:
    """Export summary metrics as JSON string."""
    return json.dumps(collector.get_summary(), indent=2)


def export_prometheus(collector: MetricsCollector) -> str:
    """Export metrics in Prometheus text exposition format."""
    summary = collector.get_summary()
    name = summary["name"]
    lines = [
        "# HELP flowguard_requests_total Total number of processed requests",
        "# TYPE flowguard_requests_total counter",
        f'flowguard_requests_total{{pipeline="{name}"}} {summary["total_requests"]}',
        f'flowguard_requests_success_total{{pipeline="{name}"}} {summary["success_count"]}',
        f'flowguard_requests_failure_total{{pipeline="{name}"}} {summary["failure_count"]}',
        f'flowguard_latency_p50_ms{{pipeline="{name}"}} {summary["latency_p50_ms"]}',
        f'flowguard_latency_p95_ms{{pipeline="{name}"}} {summary["latency_p95_ms"]}',
        f'flowguard_latency_p99_ms{{pipeline="{name}"}} {summary["latency_p99_ms"]}',
    ]
    for reason, count in summary["rejected_count"].items():
        lines.append(f'flowguard_rejected_total{{pipeline="{name}",reason="{reason}"}} {count}')

    for error_type, count in summary.get("failure_by_type", {}).items():
        lines.append(
            f'flowguard_failures_by_type_total{{pipeline="{name}",error_type="{error_type}"}} {count}'
        )

    return "\n".join(lines) + "\n"
