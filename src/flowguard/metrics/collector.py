"""Thread-safe latency and throughput telemetry metrics collector."""

import threading
import time
from typing import Any, Dict, List


class MetricsCollector:
    """Collects execution telemetry (successes, failures, rejections, latencies)."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self.total_requests = 0
        self.success_count = 0
        self.failure_count = 0
        self.rejected_count: Dict[str, int] = {
            "rate_limit": 0,
            "circuit_breaker": 0,
            "bulkhead": 0,
        }
        self.latencies: List[float] = []
        self._lock = threading.Lock()

    def record_success(self, latency: float) -> None:
        with self._lock:
            self.total_requests += 1
            self.success_count += 1
            self.latencies.append(latency)
            if len(self.latencies) > 10000:
                self.latencies = self.latencies[-5000:]

    def record_failure(self, latency: float, error_type: str) -> None:
        with self._lock:
            self.total_requests += 1
            self.failure_count += 1
            self.latencies.append(latency)
            if len(self.latencies) > 10000:
                self.latencies = self.latencies[-5000:]

    def record_rejected(self, reason: str) -> None:
        with self._lock:
            self.total_requests += 1
            self.rejected_count[reason] = self.rejected_count.get(reason, 0) + 1

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            sorted_lat = sorted(self.latencies)
            count = len(sorted_lat)

            p50 = sorted_lat[int(count * 0.50)] if count else 0.0
            p95 = sorted_lat[int(count * 0.95)] if count else 0.0
            p99 = sorted_lat[int(count * 0.99)] if count else 0.0
            avg = sum(sorted_lat) / count if count else 0.0

            return {
                "name": self.name,
                "total_requests": self.total_requests,
                "success_count": self.success_count,
                "failure_count": self.failure_count,
                "rejected_count": dict(self.rejected_count),
                "latency_avg_ms": round(avg * 1000, 2),
                "latency_p50_ms": round(p50 * 1000, 2),
                "latency_p95_ms": round(p95 * 1000, 2),
                "latency_p99_ms": round(p99 * 1000, 2),
            }

    def reset(self) -> None:
        with self._lock:
            self.total_requests = 0
            self.success_count = 0
            self.failure_count = 0
            for k in self.rejected_count:
                self.rejected_count[k] = 0
            self.latencies.clear()
