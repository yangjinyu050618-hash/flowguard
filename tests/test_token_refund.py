"""Tests for TokenBucketLimiter.refund and Prompt Caching usage reconciliation."""

import asyncio
import pytest
from flowguard.adapters.openai_compatible import ResilientOpenAICompatible
from flowguard.core.limiter import TokenBucketLimiter


@pytest.mark.asyncio
async def test_token_bucket_limiter_refund_basic():
    limiter = TokenBucketLimiter(rate=10.0, capacity=100.0)

    # Acquire 80 tokens
    success = await limiter.acquire(tokens=80.0)
    assert success is True
    assert limiter.tokens <= 21.0

    # Refund 50 tokens
    await limiter.refund(50.0)
    assert limiter.tokens >= 69.0

    # Over-refund must be capped at bucket capacity
    await limiter.refund(1000.0)
    assert limiter.tokens == 100.0

    # Zero and negative refunds are safe no-ops
    await limiter.refund(0.0)
    await limiter.refund(-50.0)
    assert limiter.tokens == 100.0


@pytest.mark.asyncio
async def test_token_bucket_refund_wakes_waiters():
    """Verify that refunding tokens immediately wakes blocked waiters in FIFO order."""
    # Rate of 1.0 token/sec, capacity 50.0. Drained to 0.
    limiter = TokenBucketLimiter(rate=1.0, capacity=50.0)

    # Drain bucket completely
    await limiter.acquire(tokens=50.0)
    assert limiter.tokens == 0.0

    waiter_completed = False

    async def waiting_worker():
        nonlocal waiter_completed
        # Would normally take 30 seconds to wake up naturally
        await limiter.acquire(tokens=30.0, timeout=60.0)
        waiter_completed = True

    task = asyncio.create_task(waiting_worker())
    await asyncio.sleep(0.02)
    assert waiter_completed is False

    # Refund 35 tokens -> waiter must be woken up immediately
    await limiter.refund(35.0)
    await asyncio.sleep(0.02)

    assert waiter_completed is True
    await task


@pytest.mark.asyncio
async def test_reconcile_usage_prompt_caching():
    """Verify reconcile_usage computes prompt cache discount and refunds unused tokens."""
    limiter = TokenBucketLimiter(rate=100.0, capacity=1000.0)
    adapter = ResilientOpenAICompatible(client=None, tpm_limit=60_000, tpm_burst_capacity=1000.0)
    adapter.tpm_limiter = limiter

    # Drained by 800 tokens (simulating estimated_tokens = 800)
    await limiter.acquire(800.0)
    tokens_before = limiter.tokens

    # Response has 500 prompt tokens (400 cached) + 100 completion tokens = 600 total
    # With 0.5 discount on cached:
    # non_cached = 100, cached_discounted = 400 * 0.5 = 200, completion = 100
    # effective = 400 tokens
    # expected refund = 800 - 400 = 400 tokens
    mock_resp = type(
        "MockResponse",
        (),
        {
            "usage": type(
                "MockUsage",
                (),
                {
                    "prompt_tokens": 500,
                    "completion_tokens": 100,
                    "total_tokens": 600,
                    "prompt_tokens_details": type("Details", (), {"cached_tokens": 400})(),
                },
            )()
        },
    )()

    await adapter.reconcile_usage(estimated_tokens=800, response=mock_resp, cache_discount=0.5)

    assert limiter.tokens >= tokens_before + 399.0
