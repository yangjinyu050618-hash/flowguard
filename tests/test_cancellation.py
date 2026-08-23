"""Async cancellation and resource leak safety tests."""

import asyncio
import pytest
from flowguard.core.limiter import TokenBucketLimiter, SlidingWindowLimiter
from flowguard.core.bulkhead import Bulkhead


async def test_token_bucket_cancellation_no_leak():
    lim = TokenBucketLimiter(rate=1.0, capacity=1.0, initial_tokens=0.0)
    
    async def task_to_cancel():
        await lim.acquire(tokens=1.0)

    t = asyncio.create_task(task_to_cancel())
    await asyncio.sleep(0.01)
    assert len(lim._waiters) == 1

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    # Waiter must be removed or marked cancelled, and tokens refill normally
    await asyncio.sleep(0.05)
    assert t.cancelled()
    # Ensure next acquisition works cleanly without deadlock
    lim.tokens = 1.0
    assert lim.try_acquire(1.0) is True


async def test_sliding_window_cancellation_no_leak():
    lim = SlidingWindowLimiter(max_requests=1, window_seconds=0.5)
    lim.try_acquire(1)

    async def task_to_cancel():
        await lim.acquire(tokens=1)

    t = asyncio.create_task(task_to_cancel())
    await asyncio.sleep(0.01)
    assert len(lim._waiters) == 1

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert t.cancelled()


async def test_bulkhead_cancellation_no_slot_leak():
    bh = Bulkhead(max_concurrent=1, max_queued=5)

    # Saturate active slot
    active_release = asyncio.Event()

    async def active_task():
        async with bh:
            await active_release.wait()

    t_active = asyncio.create_task(active_task())
    await asyncio.sleep(0.01)
    assert bh.active_count == 1
    assert bh.queued_count == 0

    # Queue a second task and cancel while waiting in queue
    async def queued_task():
        async with bh:
            pass

    t_queued = asyncio.create_task(queued_task())
    await asyncio.sleep(0.01)
    assert bh.queued_count == 1

    t_queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t_queued

    assert bh.queued_count == 0
    assert bh.active_count == 1

    # Release active and verify subsequent acquisition succeeds
    active_release.set()
    await t_active
    assert bh.active_count == 0

    async with bh:
        assert bh.active_count == 1
    assert bh.active_count == 0
