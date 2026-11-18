"""
Unit and integration tests for RedisTokenBucketLimiter.

Tests distributed token bucket rate limiting using FakeRedisClient and mock sockets:
- Zero-dependency import & friendly ImportError handling
- Atomic acquire & try_acquire
- Timeout enforcement & RateLimitExceededError retry_after
- Refund & prompt caching reconciliation
- Three-state degradation: 'fallback_local' (with local_capacity_ratio), 'fail_open', 'fail_closed'
- Waiter draining on fallback refund
- Reconnect recovery and self-healing
- Concurrency stress and contract compliance
"""

import asyncio
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

from flowguard.backends.redis import (
    LUA_ACQUIRE_SCRIPT,
    LUA_REFUND_SCRIPT,
    LUA_RESET_SCRIPT,
    RedisTokenBucketLimiter,
)
from flowguard.core.limiter import BaseRateLimiter
from flowguard.exceptions import RateLimitExceededError


class FakeRedisClient:
    """
    In-memory stateful Fake Redis client faithfully emulating Redis Lua scripts
    for RedisTokenBucketLimiter without requiring a live Redis server.
    """

    def __init__(self) -> None:
        self.storage: Dict[str, Dict[str, str]] = {}
        self.eval_calls: List[Tuple[str, int, str, Tuple[Any, ...]]] = []
        self.should_fail: bool = False
        self.fail_exception: Exception = ConnectionError("Mock Redis network connection lost")
        self.closed: bool = False

    async def eval(self, script: str, numkeys: int, key: str, *args: Any) -> Any:
        self.eval_calls.append((script, numkeys, key, args))
        if self.should_fail:
            raise self.fail_exception

        # Route to appropriate simulated Lua handler
        if script == LUA_ACQUIRE_SCRIPT:
            return self._eval_acquire(key, *args)
        elif script == LUA_REFUND_SCRIPT:
            return self._eval_refund(key, *args)
        elif script == LUA_RESET_SCRIPT:
            return self._eval_reset(key, *args)
        else:
            raise NotImplementedError(
                f"Unsupported Lua script in FakeRedisClient: {script[:30]}..."
            )

    def _eval_acquire(
        self,
        key: str,
        requested_str: str,
        rate_str: str,
        capacity_str: str,
        now_str: str,
        initial_tokens_str: Optional[str] = None,
    ) -> List[Any]:
        requested = float(requested_str)
        rate = float(rate_str)
        capacity = float(capacity_str)
        now = float(now_str) if float(now_str) > 0 else time.time()

        bucket = self.storage.get(key)
        if bucket is None:
            init = (
                min(capacity, float(initial_tokens_str))
                if initial_tokens_str is not None
                else capacity
            )
            tokens = init
            last_refill = now
        else:
            tokens = float(bucket["tokens"])
            last_refill = float(bucket.get("last_refill", str(now)))
            elapsed = max(0.0, now - last_refill)
            tokens = min(capacity, tokens + elapsed * rate)
            last_refill = now

        allowed = 0
        wait_time = 0.0

        if tokens >= requested:
            tokens -= requested
            allowed = 1
            wait_time = 0.0
        else:
            allowed = 0
            deficit = requested - tokens
            wait_time = deficit / rate

        self.storage[key] = {
            "tokens": str(tokens),
            "last_refill": str(last_refill),
        }
        return [allowed, str(tokens), str(wait_time)]

    def _eval_refund(
        self,
        key: str,
        refund_str: str,
        capacity_str: str,
        rate_str: str,
        now_str: str,
    ) -> str:
        refund_tokens = float(refund_str)
        capacity = float(capacity_str)
        rate = float(rate_str)
        now = float(now_str) if float(now_str) > 0 else time.time()

        bucket = self.storage.get(key)
        if bucket is None:
            tokens = capacity
            last_refill = now
        else:
            tokens = float(bucket["tokens"])
            last_refill = float(bucket.get("last_refill", str(now)))
            elapsed = max(0.0, now - last_refill)
            tokens = min(capacity, tokens + elapsed * rate)
            tokens = min(capacity, tokens + refund_tokens)
            last_refill = now

        self.storage[key] = {
            "tokens": str(tokens),
            "last_refill": str(last_refill),
        }
        return str(tokens)

    def _eval_reset(self, key: str, capacity_str: str, now_str: str) -> str:
        capacity = float(capacity_str)
        now = float(now_str) if float(now_str) > 0 else time.time()
        self.storage[key] = {
            "tokens": str(capacity),
            "last_refill": str(now),
        }
        return str(capacity)

    async def aclose(self) -> None:
        self.closed = True

    async def close(self) -> None:
        self.closed = True


# -----------------------------------------------------------------------------
# 1. Zero Dependency & Missing Package Tests
# -----------------------------------------------------------------------------
def test_zero_dependency_import_error():
    """Verify friendly ImportError is raised when instantiating without client and redis missing."""
    with patch.dict("sys.modules", {"redis": None, "redis.asyncio": None}):
        with pytest.raises(ImportError) as exc_info:
            RedisTokenBucketLimiter(rate=10.0, capacity=20.0, client=None)
        assert "redis package is required" in str(exc_info.value)
        assert "pip install redis" in str(exc_info.value)


def test_custom_client_works_without_redis_installed():
    """Verify RedisTokenBucketLimiter instantiates cleanly when client is provided, even if redis is missing."""
    fake_client = FakeRedisClient()
    with patch.dict("sys.modules", {"redis": None, "redis.asyncio": None}):
        limiter = RedisTokenBucketLimiter(rate=5.0, capacity=10.0, client=fake_client)
        assert limiter.rate == 5.0
        assert limiter.capacity == 10.0


# -----------------------------------------------------------------------------
# 2. Contract Compliance & Parameter Validation Tests
# -----------------------------------------------------------------------------
def test_base_rate_limiter_interface():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=20.0, client=fake_client)
    assert isinstance(limiter, BaseRateLimiter)


def test_invalid_parameters():
    fake_client = FakeRedisClient()
    with pytest.raises(ValueError, match="Rate must be positive"):
        RedisTokenBucketLimiter(rate=0.0, capacity=10.0, client=fake_client)
    with pytest.raises(ValueError, match="Rate must be positive"):
        RedisTokenBucketLimiter(rate=-5.0, capacity=10.0, client=fake_client)
    with pytest.raises(ValueError, match="Capacity must be positive"):
        RedisTokenBucketLimiter(rate=10.0, capacity=0.0, client=fake_client)
    with pytest.raises(ValueError, match="Invalid on_redis_error"):
        RedisTokenBucketLimiter(
            rate=10.0, capacity=10.0, client=fake_client, on_redis_error="invalid_mode"
        )
    with pytest.raises(ValueError, match="local_capacity_ratio must be positive"):
        RedisTokenBucketLimiter(
            rate=10.0, capacity=10.0, client=fake_client, local_capacity_ratio=0.0
        )
    with pytest.raises(ValueError, match="recovery_interval must be non-negative"):
        RedisTokenBucketLimiter(
            rate=10.0, capacity=10.0, client=fake_client, recovery_interval=-1.0
        )


def test_custom_key_and_name():
    fake_client = FakeRedisClient()
    limiter1 = RedisTokenBucketLimiter(rate=10.0, capacity=20.0, client=fake_client, name="gpt4")
    assert limiter1.key == "flowguard:limiter:gpt4"

    limiter2 = RedisTokenBucketLimiter(
        rate=10.0, capacity=20.0, client=fake_client, key="custom:org:123"
    )
    assert limiter2.key == "custom:org:123"


# -----------------------------------------------------------------------------
# 3. Core Acquisition & Try_Acquire Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_acquire_basic():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=20.0, client=fake_client)

    # Acquire 5 tokens
    success = await limiter.acquire(5.0)
    assert success is True
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(15.0, abs=0.1)

    # Acquire 15 tokens -> bucket now empty
    success = await limiter.acquire(15.0)
    assert success is True
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(0.0, abs=0.1)


@pytest.mark.asyncio
async def test_acquire_exceed_capacity():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=20.0, client=fake_client)

    with pytest.raises(ValueError, match="exceeds bucket capacity"):
        await limiter.acquire(25.0)

    with pytest.raises(ValueError, match="must be positive"):
        await limiter.acquire(0.0)

    with pytest.raises(ValueError, match="must be positive"):
        await limiter.acquire(-5.0)


@pytest.mark.asyncio
async def test_try_acquire():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=10.0, client=fake_client)

    # Synchronous try_acquire check
    assert limiter.try_acquire(6.0) is True
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(4.0, abs=0.1)

    # Remaining is ~4, trying to acquire 5 should return False
    assert limiter.try_acquire(5.0) is False
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(4.0, abs=0.1)

    # Trying to acquire 4 should succeed
    assert limiter.try_acquire(4.0) is True
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(0.0, abs=0.1)

    # Now empty
    assert limiter.try_acquire(1.0) is False

    # Exceed capacity returns False
    assert limiter.try_acquire(20.0) is False

    # Non-positive tokens raises ValueError
    with pytest.raises(ValueError, match="must be positive"):
        limiter.try_acquire(0.0)


@pytest.mark.asyncio
async def test_async_try_acquire():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=10.0, client=fake_client)

    assert await limiter.async_try_acquire(7.0) is True
    assert await limiter.async_try_acquire(5.0) is False
    assert await limiter.async_try_acquire(3.0) is True
    assert await limiter.async_try_acquire(1.0) is False


# -----------------------------------------------------------------------------
# 4. Timeout Handling Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_acquire_timeout_exhausted():
    fake_client = FakeRedisClient()
    # Rate: 1 token/sec, capacity: 5 tokens, initially 0
    limiter = RedisTokenBucketLimiter(
        rate=1.0, capacity=5.0, initial_tokens=0.0, client=fake_client
    )

    # 3 tokens require 3.0s refill. With timeout of 0.05s, must raise RateLimitExceededError
    with pytest.raises(RateLimitExceededError) as exc_info:
        await limiter.acquire(tokens=3.0, timeout=0.05)

    assert exc_info.value.retry_after is not None
    assert exc_info.value.retry_after >= 2.9


@pytest.mark.asyncio
async def test_acquire_timeout_sufficient_refill():
    fake_client = FakeRedisClient()
    # Rate: 100 tokens/sec, capacity: 10 tokens, initially 0
    limiter = RedisTokenBucketLimiter(
        rate=100.0, capacity=10.0, initial_tokens=0.0, client=fake_client
    )

    # Acquiring 2 tokens takes 0.02s. With timeout of 0.2s, should successfully wait and acquire
    with patch("flowguard.backends.redis.time") as clock:
        clock.time.side_effect = [1.0, 1.03125]
        clock.monotonic.return_value = 1.0
        with patch("flowguard.backends.redis.asyncio.sleep") as sleep:
            success = await limiter.acquire(tokens=2.0, timeout=0.2)

    assert success is True
    sleep.assert_awaited_once_with(pytest.approx(0.02))
    assert len(fake_client.eval_calls) == 2


# -----------------------------------------------------------------------------
# 5. Refund & Prompt Caching Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_refund_basic():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=50.0, client=fake_client)

    await limiter.acquire(40.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) == 10.0

    # Refund 20 tokens
    await limiter.refund(20.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) >= 30.0

    # Over-refund must be capped at capacity (50.0)
    await limiter.refund(100.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) == 50.0

    # Zero or negative refund are no-ops
    await limiter.refund(0.0)
    await limiter.refund(-10.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) == 50.0


@pytest.mark.asyncio
async def test_refund_sync():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=30.0, client=fake_client)

    await limiter.acquire(25.0)
    limiter.refund_sync(15.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) >= 20.0


@pytest.mark.asyncio
async def test_refund_wakes_waiters_on_fallback():
    """Verify that in fallback mode, refund() immediately wakes pending waiters."""
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(
        rate=1.0, capacity=50.0, client=fake_client, on_redis_error="fallback_local"
    )

    # Force fallback mode
    limiter.set_fallback(True)

    # Drain local bucket
    await limiter.acquire(50.0)
    assert limiter.local_limiter.tokens == 0.0

    waiter_completed = False

    async def waiting_coro():
        nonlocal waiter_completed
        # Would normally take 20s to acquire naturally; timeout=60s ensures no premature timeout
        await limiter.acquire(20.0, timeout=60.0)
        waiter_completed = True

    task = asyncio.create_task(waiting_coro())
    await asyncio.sleep(0.02)
    assert waiter_completed is False

    # Refund 25 tokens -> must wake waiter immediately
    await limiter.refund(25.0)
    await asyncio.sleep(0.02)
    assert waiter_completed is True
    await task


# -----------------------------------------------------------------------------
# 6. Reset Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reset():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=25.0, client=fake_client)

    await limiter.acquire(20.0)
    assert float(fake_client.storage[limiter.key]["tokens"]) == 5.0

    await limiter.reset()
    assert float(fake_client.storage[limiter.key]["tokens"]) == 25.0


# -----------------------------------------------------------------------------
# 7. Degradation on Redis Disconnect Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_degradation_fallback_local():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(
        rate=10.0,
        capacity=20.0,
        client=fake_client,
        on_redis_error="fallback_local",
        local_capacity_ratio=0.5,
    )

    # Healthy call succeeds via Redis
    assert await limiter.acquire(5.0) is True
    assert not limiter.is_fallback_active

    # Simulate Redis connection failure
    fake_client.should_fail = True
    fake_client.fail_exception = ConnectionResetError("Redis socket abruptly closed")

    # Call should not raise; degrades to local in-memory TokenBucketLimiter
    assert await limiter.acquire(5.0) is True
    assert limiter.is_fallback_active is True

    # Local capacity was scaled by 0.5: 20 * 0.5 = 10 capacity, 10 * 0.5 = 5 rate
    assert limiter.local_limiter.capacity == 10.0
    assert limiter.local_limiter.rate == 5.0

    # Local try_acquire should work
    assert limiter.try_acquire(3.0) is True


@pytest.mark.asyncio
async def test_degradation_fail_open():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(
        rate=5.0, capacity=10.0, client=fake_client, on_redis_error="fail_open"
    )

    fake_client.should_fail = True
    fake_client.fail_exception = TimeoutError("Redis connection timed out")

    # In fail_open mode, all acquires and try_acquires succeed immediately
    assert await limiter.acquire(10.0) is True
    assert limiter.try_acquire(10.0) is True
    assert limiter.is_fallback_active is True


@pytest.mark.asyncio
async def test_degradation_fail_closed():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(
        rate=5.0, capacity=10.0, client=fake_client, on_redis_error="fail_closed"
    )

    fake_client.should_fail = True
    fake_client.fail_exception = ConnectionRefusedError("Redis server refused connection")

    # In fail_closed mode, try_acquire returns False and acquire raises RateLimitExceededError
    assert limiter.try_acquire(1.0) is False
    with pytest.raises(RateLimitExceededError, match="fail_closed"):
        await limiter.acquire(1.0)


# -----------------------------------------------------------------------------
# 8. Mock Socket & Network Error Simulation Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_mock_socket_network_exceptions():
    """Verify low-level socket transport errors trigger degradation."""
    socket_errors = [
        socket.error("Socket error 10054"),
        BrokenPipeError("Broken pipe"),
        ConnectionAbortedError("Connection aborted"),
        OSError("Network is unreachable"),
    ]

    for err in socket_errors:
        fake_client = FakeRedisClient()
        limiter = RedisTokenBucketLimiter(
            rate=10.0, capacity=20.0, client=fake_client, on_redis_error="fallback_local"
        )
        fake_client.should_fail = True
        fake_client.fail_exception = err

        # Should cleanly degrade without crashing
        success = await limiter.acquire(1.0)
        assert success is True
        assert limiter.is_fallback_active is True


# -----------------------------------------------------------------------------
# 9. Self-Healing & Reconnect Recovery Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
@patch("flowguard.backends.redis.time", wraps=time)
async def test_reconnect_recovery(clock):
    clock.monotonic.return_value = 1.0
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(
        rate=10.0,
        capacity=20.0,
        client=fake_client,
        recovery_interval=1.0,
        on_redis_error="fallback_local",
    )

    # 1. Successful Redis call
    await limiter.acquire(2.0)
    assert not limiter.is_fallback_active

    # 2. Redis fails -> fallback triggered
    fake_client.should_fail = True
    await limiter.acquire(2.0)
    assert limiter.is_fallback_active is True

    # 3. Redis comes back online
    fake_client.should_fail = False

    calls_after_failure = len(fake_client.eval_calls)
    clock.monotonic.return_value = 1.99
    await limiter.acquire(1.0)
    assert limiter.is_fallback_active
    assert len(fake_client.eval_calls) == calls_after_failure

    # At the cooldown boundary the next acquire probes Redis exactly once.
    clock.monotonic.return_value = 2.0
    await limiter.acquire(2.0)
    assert len(fake_client.eval_calls) == calls_after_failure + 1
    assert limiter.is_fallback_active is False


# -----------------------------------------------------------------------------
# 10. Concurrency & Stress Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_acquisitions():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=100.0, capacity=50.0, client=fake_client)

    # 25 tasks each acquiring 2 tokens = 50 tokens (exact bucket capacity)
    results = await asyncio.gather(*[limiter.acquire(2.0) for _ in range(25)])
    assert all(results)
    assert float(fake_client.storage[limiter.key]["tokens"]) == pytest.approx(0.0, abs=0.5)


# -----------------------------------------------------------------------------
# 11. Cleanup & Close Tests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_limiter_close():
    fake_client = FakeRedisClient()
    limiter = RedisTokenBucketLimiter(rate=10.0, capacity=10.0, client=fake_client)

    # Use sync try_acquire to initialize sync executor
    limiter.try_acquire(1.0)
    assert limiter._sync_executor is not None

    await limiter.aclose()
    assert limiter._sync_executor is None
