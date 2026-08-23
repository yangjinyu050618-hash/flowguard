import asyncio
import pytest
from flowguard.core.limiter import TokenBucketLimiter, SlidingWindowLimiter
from flowguard.exceptions import RateLimitExceededError


@pytest.mark.asyncio
async def test_token_bucket_acquire():
    limiter = TokenBucketLimiter(rate=100.0, capacity=10.0)
    assert limiter.try_acquire(5.0) is True
    assert limiter.try_acquire(5.0) is True
    assert limiter.try_acquire(1.0) is False

    # Wait for refill
    await asyncio.sleep(0.05)
    assert limiter.try_acquire(2.0) is True


@pytest.mark.asyncio
async def test_token_bucket_timeout():
    limiter = TokenBucketLimiter(rate=1.0, capacity=1.0, initial_tokens=0.0)
    with pytest.raises(RateLimitExceededError):
        await limiter.acquire(tokens=1.0, timeout=0.05)


@pytest.mark.asyncio
async def test_token_bucket_exceed_capacity():
    limiter = TokenBucketLimiter(rate=10.0, capacity=5.0)
    with pytest.raises(ValueError):
        await limiter.acquire(tokens=10.0)


@pytest.mark.asyncio
async def test_sliding_window_limiter():
    limiter = SlidingWindowLimiter(max_requests=2, window_seconds=0.2)
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is False

    await asyncio.sleep(0.25)
    assert limiter.try_acquire() is True


@pytest.mark.asyncio
async def test_sliding_window_exceed_max_requests():
    limiter = SlidingWindowLimiter(max_requests=3, window_seconds=1.0)
    with pytest.raises(ValueError):
        await limiter.acquire(tokens=5)
