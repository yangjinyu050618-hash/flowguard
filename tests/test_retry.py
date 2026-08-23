import pytest
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.exceptions import MaxRetriesExceededError, CircuitBreakerOpenError


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
    assert isinstance(exc_info.value.__cause__, KeyError)


@pytest.mark.asyncio
async def test_retry_policy_fatal_exception_not_retried():
    attempts = 0

    async def open_breaker_call():
        nonlocal attempts
        attempts += 1
        raise CircuitBreakerOpenError("circuit open")

    policy = RetryPolicy(
        max_attempts=5,
        backoff=ExponentialBackoff(base_delay=0.01, jitter="none"),
    )
    with pytest.raises(CircuitBreakerOpenError):
        await policy.execute(open_breaker_call)
    assert attempts == 1


@pytest.mark.asyncio
async def test_decorrelated_jitter():
    backoff = ExponentialBackoff(base_delay=0.1, max_delay=1.0, jitter="decorrelated")
    d1 = backoff.compute_delay(1, 0.0)
    assert 0.1 <= d1 <= 1.0
