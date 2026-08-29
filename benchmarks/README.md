# FlowGuard Behavioral Resilience Benchmarks

This directory contains deterministic, zero-network, zero-dependency behavioral fault-injection benchmarks for FlowGuard.

---

## ⚠️ Important Disclaimer

> **Scope**: This is a **deterministic behavioral regression benchmark**, NOT an external vendor throughput leaderboard, and it does not replace production load testing with real network links.
> 
> Its primary purpose is to mathematically verify that FlowGuard's rate limiting, retry backoff, circuit breaking state transitions, fallback routing, and task cancellation interact correctly and preserve all request accounting invariants under concurrent faults.

---

## 🏃 Running the Benchmark

```bash
python benchmarks/resilience_scenario.py
```

### Example JSON Output:
```json
{
  "total_requests": 50,
  "successful_requests": 36,
  "primary_calls": 58,
  "retried_calls": 5,
  "fallback_calls": 11,
  "cancelled_requests": 3,
  "exceptions_caught": 0,
  "circuit_state_final": "CLOSED",
  "latency_p50_ms": 2.51,
  "latency_p95_ms": 7.82
}
```

### Verified Invariants:
1. **Request Conservation**: `successful_requests + fallback_calls + cancelled_requests + exceptions_caught == total_requests`.
2. **Zero-Leak Cancellation**: `asyncio.CancelledError` on hanging tasks unwinds immediately without slot or token leaks.
3. **Circuit Self-Healing**: Trips `OPEN` under burst failures, routes gracefully to fallback, allows gated probes in `HALF_OPEN`, and recovers automatically to `CLOSED`.
4. **Retry Accounting**: Transient errors are retried up to policy budget without exceeding max attempts.
