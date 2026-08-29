"""
Deterministic Resilience Scenario Benchmark for FlowGuard.

This script executes a reproducible, zero-network, zero-API-key fault injection sequence
to verify rate limiting, retry backoff, circuit breaking, fallback routing, and task cancellation.
Outputs machine-readable JSON metrics and self-asserts invariant consistency.
"""
# ruff: noqa: E402

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

# Ensure src/ is on sys.path for standalone invocation
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from flowguard.core.bulkhead import Bulkhead
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.core.fallback import ChoiceFallback, FallbackContext
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.pipeline import FlowGuard
from flowguard.core.retry import ExponentialBackoff, RetryPolicy

# Suppress debug logs during benchmark execution
logging.basicConfig(level=logging.ERROR)


class MockResilientService:
    """Deterministic mock service generating controlled failure sequences."""

    def __init__(self) -> None:
        self.downstream_invocations: int = 0
        self.transient_fail_tracker: Dict[int, int] = {}

    async def execute(self, req_id: int, mode: str) -> str:
        self.downstream_invocations += 1
        await asyncio.sleep(0.001)

        if mode == "success":
            return f"success_{req_id}"

        if mode == "transient_fail":
            attempts = self.transient_fail_tracker.get(req_id, 0) + 1
            self.transient_fail_tracker[req_id] = attempts
            if attempts == 1:
                raise ConnectionResetError(f"Transient 503 for req {req_id} attempt {attempts}")
            return f"recovered_{req_id}"

        if mode == "hard_fail":
            raise ConnectionRefusedError(f"Upstream cluster outage 502 for req {req_id}")

        if mode == "hanging":
            await asyncio.sleep(5.0)
            return f"hung_{req_id}"

        return f"default_{req_id}"


async def run_scenario() -> Dict[str, Any]:
    """Run the deterministic resilience benchmark scenario."""
    service = MockResilientService()
    retry_dispatches: int = 0

    def _on_retry(attempt: int, exc: BaseException, delay: float) -> None:
        nonlocal retry_dispatches
        retry_dispatches += 1

    cb = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=0.2,
        half_open_success_threshold=2,
        half_open_max_probes=2,
    )
    retry = RetryPolicy(
        max_attempts=2,
        backoff=ExponentialBackoff(base_delay=0.001, jitter="none"),
        on_retry=_on_retry,
    )
    limiter = TokenBucketLimiter(rate=500.0, capacity=50.0, initial_tokens=50.0)
    bulkhead = Bulkhead(max_concurrent=10, max_queued=20)

    def _backup_candidate(ctx: FallbackContext) -> str:
        req_id = ctx.args[0] if ctx.args else "unknown"
        return f"fallback_handled_{req_id}"

    fallback_router = ChoiceFallback(
        candidates={
            "backup-model": _backup_candidate,
        },
        selector=lambda ctx, opts: "backup-model",
    )

    pipeline = FlowGuard(
        name="resilience-benchmark",
        limiter=limiter,
        circuit_breaker=cb,
        retry=retry,
        bulkhead=bulkhead,
        fallback=fallback_router,
    )

    total_requests = 50
    successful_requests = 0
    fallback_calls = 0
    cancelled_requests = 0
    exceptions_caught = 0
    latencies: List[float] = []

    # Deterministic Execution Sequence:
    # Req 0-9   (10): Normal requests (10 successes, 10 downstream calls)
    # Req 10-12  (3): Hanging requests cancelled mid-flight (3 cancellations, 3 downstream calls)
    # Req 13-17  (5): Transient fails -> 1st attempt fails, 2nd succeeds (5 successes, 10 downstream calls, 5 retries)
    # Req 18-29 (12): Hard fails -> 18 fails 2 attempts (2 calls, 1 retry, CB fails=1); 19 fails attempt 1 (1 call, 1 retry, CB fails=2); attempt 2 trips CB to OPEN (fail-fast, 0 calls); 20..29 fail fast while OPEN (0 calls, 0 retries). (12 fallbacks, 3 downstream calls, 2 retries)
    # Cooldown sleep (0.3s > recovery_timeout 0.2s) -> CB enters HALF_OPEN
    # Req 30-31  (2): Half-open probes succeed -> CB closes to CLOSED (2 successes, 2 downstream calls)
    # Req 32-49 (18): Healthy requests (18 successes, 18 downstream calls)

    for req_id in range(total_requests):
        t0 = time.monotonic()
        mode = "success"

        if 10 <= req_id <= 12:
            mode = "hanging"
        elif 13 <= req_id <= 17:
            mode = "transient_fail"
        elif 18 <= req_id <= 29:
            mode = "hard_fail"
        elif req_id == 30:
            # Let circuit breaker recovery cooldown elapse before probe requests
            await asyncio.sleep(0.3)
            mode = "success"
        else:
            mode = "success"

        if mode == "hanging":
            task = asyncio.create_task(pipeline.execute(service.execute, req_id, mode))
            await asyncio.sleep(0.005)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                cancelled_requests += 1
                latencies.append((time.monotonic() - t0) * 1000.0)
            continue

        try:
            res = await pipeline.execute(service.execute, req_id, mode)
            latencies.append((time.monotonic() - t0) * 1000.0)

            if isinstance(res, str) and res.startswith("fallback_handled_"):
                fallback_calls += 1
            else:
                successful_requests += 1
        except Exception:
            exceptions_caught += 1
            latencies.append((time.monotonic() - t0) * 1000.0)

    # Compute latency percentiles
    sorted_latencies = sorted(latencies)
    p50_idx = int(len(sorted_latencies) * 0.50)
    p95_idx = int(len(sorted_latencies) * 0.95)
    p50_ms = round(sorted_latencies[p50_idx], 2) if sorted_latencies else 0.0
    p95_ms = round(sorted_latencies[p95_idx], 2) if sorted_latencies else 0.0

    result = {
        "total_requests": total_requests,
        "successful_requests": successful_requests,
        "downstream_invocations": service.downstream_invocations,
        "retried_calls": retry_dispatches,
        "fallback_calls": fallback_calls,
        "cancelled_requests": cancelled_requests,
        "exceptions_caught": exceptions_caught,
        "circuit_state_final": cb.state.value,
        "latency_p50_ms": p50_ms,
        "latency_p95_ms": p95_ms,
    }

    # Self-assert critical behavioral invariants
    assert (
        successful_requests + fallback_calls + cancelled_requests + exceptions_caught
        == total_requests
    ), f"Request accounting mismatch: {result}"
    assert successful_requests == 35, f"Expected 35 successful requests, got {successful_requests}"
    assert fallback_calls == 12, f"Expected 12 fallback calls, got {fallback_calls}"
    assert cancelled_requests == 3, f"Expected 3 cancelled requests, got {cancelled_requests}"
    assert retry_dispatches == 7, (
        f"Expected exactly 7 retry dispatches (5 transient + 2 hard before OPEN), got {retry_dispatches}"
    )
    assert service.downstream_invocations == 46, (
        f"Expected 46 total downstream invocations, got {service.downstream_invocations}"
    )
    assert cb.state == CircuitState.CLOSED, "Expected circuit breaker to recover to CLOSED state"
    assert p50_ms >= 0.0, "Expected non-negative p50 latency"
    assert p95_ms >= p50_ms, "Expected p95 latency to be >= p50 latency"

    return result


def main() -> None:
    try:
        data = asyncio.run(run_scenario())
        print(json.dumps(data, indent=2))
        sys.exit(0)
    except AssertionError as err:
        print(json.dumps({"error": "InvariantViolation", "detail": str(err)}), file=sys.stderr)
        sys.exit(1)
    except Exception as err:
        print(json.dumps({"error": "ExecutionFailure", "detail": str(err)}), file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
