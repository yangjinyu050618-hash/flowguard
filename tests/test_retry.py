import asyncio
import pytest
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.exceptions import MaxRetriesExceededError


@pytest.mark.asyncio
async def test_retry_policy_success():
    attempts = 0

    async def flaky_op():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("temporary error")
        return "success"

    policy = RetryPolicy(
        max_attempts=3,
        backoff=ExponentialBackoff(base_delay=0.01, multiplier=1.5, jitter="none"),
    )
    result = await policy.execute(flaky_op)
    assert result == "success"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_policy_max_exhausted():
    async def always_fails():
        raise KeyError("always fails")

    policy = RetryPolicy(
        max_attempts=2,
        backoff=ExponentialBackoff(base_delay=0.01, jitter="none"),
    )
    with pytest.raises(MaxRetriesExceededError) as exc_info:
        await policy.execute(always_fails)
    assert exc_info.value.attempts == 2
