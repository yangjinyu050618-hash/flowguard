"""High-concurrency stress, admission rate, and stampede tests."""

import asyncio
import time
from flowguard.core.limiter import TokenBucketLimiter, SlidingWindowLimiter
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState


async def test_token_bucket_stress_rate_enforcement():
    # 50 requests with rate 100/s -> expected duration >= ~0.45s
    rate = 100.0
    total = 30
    lim = TokenBucketLimiter(rate=rate, capacity=5.0, initial_tokens=5.0)

    start = time.monotonic()
    completed = 0

    async def worker():
        nonlocal completed
        await lim.acquire(tokens=1.0)
        completed += 1

    await asyncio.gather(*[worker() for _ in range(total)])
    elapsed = time.monotonic() - start

    assert completed == total
    # 30 tokens with initial 5 -> 25 needed -> 25 / 100 = 0.25s (allow margin for scheduler jitter)
    assert elapsed >= 0.15


async def test_sliding_window_stress():
    # Window of max 5 reqs per 0.1s
    lim = SlidingWindowLimiter(max_requests=5, window_seconds=0.1)
    completed = 0

    async def worker():
        nonlocal completed
        await lim.acquire(tokens=1)
        completed += 1

    await asyncio.gather(*[worker() for _ in range(15)])
    assert completed == 15


async def test_circuit_breaker_half_open_stampede_protection():
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.03,
        half_open_success_threshold=3,
        half_open_max_probes=3,
    )
    await cb.record_failure(RuntimeError("trip"))
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.08)

    admitted = 0
    rejected = 0

    async def probe():
        nonlocal admitted, rejected
        try:
            await cb.before_call()
            admitted += 1
        except Exception:
            rejected += 1

    await asyncio.gather(*[probe() for _ in range(50)])

    assert admitted <= 3
    assert rejected >= 47
