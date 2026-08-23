"""Pipeline integration, retry per attempt, and telemetry tests."""

import pytest
from flowguard.core.pipeline import FlowGuard, guard
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.bulkhead import Bulkhead
from flowguard.exceptions import CircuitBreakerOpenError, BulkheadFullError


async def test_pipeline_retry_token_consumption_and_metrics():
    lim = TokenBucketLimiter(rate=0.01, capacity=10.0, initial_tokens=10.0)
    attempts = 0

    async def failing_service():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient 503")
        return "success"

    pipeline = FlowGuard(
        name="test-pipeline",
        limiter=lim,
        retry=RetryPolicy(max_attempts=4, backoff=ExponentialBackoff(base_delay=0.001, jitter="none")),
    )

    res = await pipeline.execute(failing_service)
    assert res == "success"
    assert attempts == 3
    consumed = 10.0 - lim.current_tokens
    assert consumed == pytest.approx(3.0, abs=0.1), (
        f"每次重试尝试都应独立消耗令牌：期望 3.0，实际 {consumed:.2f}"
    )
    summary = pipeline.metrics.get_summary()
    assert summary["success_count"] == 1
    assert summary["failure_count"] == 2
    assert summary["failure_by_type"]["RuntimeError"] == 2


async def test_pipeline_circuit_breaker_fast_fail_metric():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60.0)
    await cb.record_failure(RuntimeError("trip"))

    pipeline = FlowGuard(name="cb-fastfail", circuit_breaker=cb)

    with pytest.raises(CircuitBreakerOpenError):
        await pipeline.execute(lambda: None)

    summary = pipeline.metrics.get_summary()
    assert summary["rejected_count"]["circuit_breaker"] == 1


async def test_pipeline_bulkhead_saturation_metric():
    bh = Bulkhead(max_concurrent=1, max_queued=0)
    
    pipeline = FlowGuard(name="bh-pipeline", bulkhead=bh)

    import asyncio
    active_gate = asyncio.Event()

    async def blocking_call():
        await active_gate.wait()

    t = asyncio.create_task(pipeline.execute(blocking_call))
    await asyncio.sleep(0.01)

    # Next call should be rejected by bulkhead
    with pytest.raises(BulkheadFullError):
        await pipeline.execute(lambda: None)

    summary = pipeline.metrics.get_summary()
    assert summary["rejected_count"]["bulkhead"] == 1

    active_gate.set()
    await t


async def test_guard_decorator_type_error():
    with pytest.raises(TypeError):
        @guard(name="sync-not-allowed")
        def sync_func():
            pass
