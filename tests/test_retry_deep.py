"""Deep tests for RetryPolicy callbacks, exceptions, and jitter algorithms."""

import pytest
from flowguard.core.retry import RetryPolicy, ExponentialBackoff


async def test_retry_on_retry_callback_sync_and_async():
    callbacks = []

    def sync_cb(attempt, exc, delay):
        callbacks.append((attempt, type(exc).__name__, delay))

    attempts = 0

    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ValueError("flaky")
        return "ok"

    policy = RetryPolicy(
        max_attempts=3,
        backoff=ExponentialBackoff(base_delay=0.001, jitter="none"),
        on_retry=sync_cb,
    )
    res = await policy.execute(flaky)
    assert res == "ok"
    assert len(callbacks) == 1
    assert callbacks[0][0] == 1
    assert callbacks[0][1] == "ValueError"


async def test_retry_reraise_original():
    async def always_custom_error():
        raise KeyError("custom-key-error")

    policy = RetryPolicy(
        max_attempts=2,
        backoff=ExponentialBackoff(base_delay=0.001, jitter="none"),
        reraise=True,
    )
    with pytest.raises(KeyError) as exc_info:
        await policy.execute(always_custom_error)
    assert "custom-key-error" in str(exc_info.value)


def test_invalid_jitter_mode():
    with pytest.raises(ValueError):
        ExponentialBackoff(jitter="invalid-mode")
