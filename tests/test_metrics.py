from flowguard.metrics.collector import MetricsCollector
from flowguard.metrics.exporter import export_json, export_prometheus


def test_metrics_collection():
    collector = MetricsCollector(name="order-api")
    collector.record_success(0.012)
    collector.record_success(0.025)
    collector.record_failure(0.050, "TimeoutError")
    collector.record_rejected("rate_limit")

    summary = collector.get_summary()
    assert summary["total_requests"] == 4
    assert summary["success_count"] == 2
    assert summary["failure_count"] == 1
    assert summary["rejected_count"]["rate_limit"] == 1

    json_str = export_json(collector)
    assert "latency_avg_ms" in json_str

    prom_str = export_prometheus(collector)
    assert "flowguard_requests_total" in prom_str
