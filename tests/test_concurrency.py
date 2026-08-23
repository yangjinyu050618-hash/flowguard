import asyncio
import pytest
from flowguard.core.limiter import TokenBucketLimiter, SlidingWindowLimiter
from flowguard.core.bulkhead import Bulkhead
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.exceptions import RateLimitExceededError, BulkheadFullError


@pytest.mark.asyncio
async def test_token_bucket_fifo_inversions():
    lim = TokenBucketLimiter(rate=50.0, capacity=1.0, initial_tokens=0.0)
    served = []

    async def worker(idx: int):
        await lim.acquire(tokens=1.0)
        served.append(idx)

    tasks = [asyncio.create_task(worker(i)) for i in range(10)]
    await asyncio.gather(*tasks)

    inversions = sum(1 for i in range(len(served)) for j in range(i + 1, len(served)) if served[i] > served[j])
    assert inversions == 0
    assert served == list(range(10))


@pytest.mark.asyncio
async def test_sliding_window_acquire_and_fifo():
    lim = SlidingWindowLimiter(max_requests=1, window_seconds=0.05)
    served = []

    async def worker(idx: int):
        await lim.acquire(tokens=1)
        served.append(idx)

    tasks = [asyncio.create_task(worker(i)) for i in range(6)]
    await asyncio.gather(*tasks)

    assert len(served) == 6
    assert served == list(range(6))


@pytest.mark.asyncio
async def test_sliding_window_timeout():
    lim = SlidingWindowLimiter(max_requests=1, window_seconds=0.5)
    await lim.acquire(tokens=1)

    with pytest.raises(RateLimitExceededError):
        await lim.acquire(tokens=1, timeout=0.05)


@pytest.mark.asyncio
async def test_limiter_resets():
    tb = TokenBucketLimiter(rate=10.0, capacity=5.0, initial_tokens=0.0)
    await tb.reset()
    assert tb.tokens == 5.0

    sw = SlidingWindowLimiter(max_requests=5, window_seconds=1.0)
    sw.try_acquire(3)
    await sw.reset()
    assert len(sw._timestamps) == 0


@pytest.mark.asyncio
async def test_bulkhead_queue_timeout():
    bh = Bulkhead(max_concurrent=1, max_queued=5, queue_timeout=0.05)

    async def long_task():
        async with bh:
            await asyncio.sleep(0.1)

    t1 = asyncio.create_task(long_task())
    await asyncio.sleep(0.01)

    with pytest.raises(BulkheadFullError):
        async with bh:
            pass

    await t1


@pytest.mark.asyncio
async def test_circuit_breaker_callbacks():
    states = []

    def on_change(old, new):
        states.append((old, new))

    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05, on_state_change=on_change)
    await cb.record_failure(RuntimeError("error"))
    assert cb.state == CircuitState.OPEN
    assert states == [(CircuitState.CLOSED, CircuitState.OPEN)]

    await cb.reset()
    assert cb.state == CircuitState.CLOSED
