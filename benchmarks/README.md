# FlowGuard Behavioral Resilience Benchmarks

This directory contains deterministic, zero-network, zero-dependency behavioral fault-injection benchmarks for FlowGuard.

---

## ⚠️ Important Disclaimer

> **Scope**: This is a **deterministic behavioral regression benchmark**, NOT an external vendor throughput leaderboard, and it does not replace production load testing with real network links.
> 
> Its primary purpose is to mathematically verify that FlowGuard's rate limiting, retry backoff, circuit breaking state transitions, fallback routing, and task cancellation interact correctly and preserve all request accounting invariants under deterministic fault injection phases (including healthy execution, hanging request cancellations, transient retries, hard failure circuit tripping, fast fallback rejections, and half-open probe recovery).

---

## 🏃 Running the Benchmark

```bash
python benchmarks/resilience_scenario.py
```

### Example JSON Output:
```json
{
  "total_requests": 50,
  "successful_requests": 35,
  "downstream_invocations": 46,
  "retried_calls": 7,
  "fallback_calls": 12,
  "cancelled_requests": 3,
  "exceptions_caught": 0,
  "circuit_state_final": "CLOSED",
  "latency_p50_ms": 16.0,
  "latency_p95_ms": 47.0
}
```

### Verified Invariants:
1. **Request Conservation**: `successful_requests + fallback_calls + cancelled_requests + exceptions_caught == total_requests` (50 total = 35 success + 12 fallback + 3 cancelled).
2. **Zero-Leak Slot Recovery**: `asyncio.CancelledError` on hanging tasks unwinds immediately without orphaning tasks or deadlocking bulkhead/circuit breaker probe slots (rate limit tokens already budgeted prior to cancellation are non-refundable).
3. **Circuit Self-Healing**: Trips `OPEN` after threshold failures, fast-rejects directly to fallback without downstream calls, allows gated probes in `HALF_OPEN`, and recovers automatically to `CLOSED`.
4. **Deterministic Retry Accounting**: Transient errors are retried up to policy budget (7 total retries dispatched: 5 transient + 2 hard failures before circuit trip; fast rejections while `OPEN` dispatch 0 retries).
