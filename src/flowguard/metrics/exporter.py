"""Prometheus and JSON metrics exporter."""

import json
from flowguard.metrics.collector import MetricsCollector


def _escape_prometheus_label(val: str) -> str:
    """Escape backslash, double-quote, and newline characters per Prometheus text exposition spec."""
    return str(val).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def export_json(collector: MetricsCollector) -> str:
    """Export summary metrics as JSON string."""
    return json.dumps(collector.get_summary(), indent=2)


def export_prometheus(collector: MetricsCollector) -> str:
    """Export metrics in Prometheus text exposition format with sanitized label values."""
    summary = collector.get_summary()
    name = _escape_prometheus_label(summary["name"])
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
        escaped_reason = _escape_prometheus_label(reason)
        lines.append(
            f'flowguard_rejected_total{{pipeline="{name}",reason="{escaped_reason}"}} {count}'
        )

    for error_type, count in summary.get("failure_by_type", {}).items():
        escaped_err = _escape_prometheus_label(error_type)
        lines.append(
            f'flowguard_failures_by_type_total{{pipeline="{name}",error_type="{escaped_err}"}} {count}'
        )

    return "\n".join(lines) + "\n"
