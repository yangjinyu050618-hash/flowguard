"""
High-performance Distributed Redis Token Bucket Rate Limiter with Atomic Lua Scripts.

Provides zero-dependency async Redis token bucket limiting, atomic remote replenishment,
sub-millisecond evaluation, strict FIFO coordination, prompt caching refunds, and three-state
degradation ('fallback_local', 'fail_open', 'fail_closed') when Redis encounters network failures.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import time
from typing import Any, Optional, Tuple

from flowguard.core.limiter import BaseRateLimiter, TokenBucketLimiter
from flowguard.exceptions import RateLimitExceededError

logger = logging.getLogger("flowguard.backends.redis")

# Atomic Redis Lua script for token replenishment and acquisition.
LUA_ACQUIRE_SCRIPT = """-- KEYS[1]: limiter key (e.g., "flowguard:limiter:default")
-- ARGV[1]: tokens requested (number)
-- ARGV[2]: rate (tokens per second)
-- ARGV[3]: capacity (maximum tokens)
-- ARGV[4]: current timestamp in seconds (optional)
-- ARGV[5]: initial tokens (optional, defaults to capacity)

local key = KEYS[1]
local requested = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

if not now or now <= 0 then
    local t = redis.call('TIME')
    now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
end

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens then
    local initial_tokens = tonumber(ARGV[5])
    if initial_tokens then
        tokens = math.min(capacity, initial_tokens)
    else
        tokens = capacity
    end
    last_refill = now
else
    if not last_refill then
        last_refill = now
    end
    local elapsed = math.max(0.0, now - last_refill)
    tokens = math.min(capacity, tokens + (elapsed * rate))
    last_refill = now
end

local allowed = 0
local wait_time = 0.0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
    wait_time = 0.0
else
    allowed = 0
    local deficit = requested - tokens
    wait_time = deficit / rate
end

redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(last_refill))

local ttl = math.ceil((capacity / rate) * 2.0) + 60
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens), tostring(wait_time)}
"""

# Atomic Redis Lua script for returning refunded tokens.
LUA_REFUND_SCRIPT = """-- KEYS[1]: limiter key
-- ARGV[1]: tokens to refund (number)
-- ARGV[2]: capacity (maximum tokens)
-- ARGV[3]: rate (tokens per second)
-- ARGV[4]: current timestamp in seconds (optional)

local key = KEYS[1]
local refund_tokens = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

if not now or now <= 0 then
    local t = redis.call('TIME')
    now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
end

if refund_tokens <= 0 then
    local current = redis.call('HGET', key, 'tokens')
    return tostring(current or capacity)
end

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_refill = now
else
    if not last_refill then
        last_refill = now
    end
    local elapsed = math.max(0.0, now - last_refill)
    tokens = math.min(capacity, tokens + (elapsed * rate))
    tokens = math.min(capacity, tokens + refund_tokens)
    last_refill = now
end

redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(last_refill))

local ttl = math.ceil((capacity / rate) * 2.0) + 60
redis.call('EXPIRE', key, ttl)

return tostring(tokens)
"""

# Atomic Redis Lua script for resetting limiter state.
LUA_RESET_SCRIPT = """-- KEYS[1]: limiter key
-- ARGV[1]: capacity (maximum tokens)
-- ARGV[2]: current timestamp in seconds (optional)

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local now = tonumber(ARGV[2])

if not now or now <= 0 then
    local t = redis.call('TIME')
    now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
end

redis.call('HMSET', key, 'tokens', tostring(capacity), 'last_refill', tostring(now))
return tostring(capacity)
"""


class RedisTokenBucketLimiter(BaseRateLimiter):
    """
    High-performance distributed Token Bucket rate limiter backed by Redis.

    Features:
    1. Zero-dependency core: imports redis.asyncio dynamically with a clear ImportError if missing.
    2. High-performance atomic Lua script for Redis-side replenishment and consumption in sub-millisecond time.
    3. Three-state degradation on Redis error ('fallback_local', 'fail_open', 'fail_closed').
    4. Support for refund() returning tokens to Redis and immediately draining local waiters on fallback.
    5. Automatic self-healing: automatically recovers from fallback once Redis connectivity resumes.

    Parameters
    ----------
    rate : float
        Refill rate (tokens per second).
    capacity : float
        Maximum token burst capacity.
    initial_tokens : Optional[float]
        Starting tokens in the bucket (defaults to capacity).
    client : Optional[Any]
        Pre-configured redis.asyncio.Redis client or compatible mock.
    redis_url : Optional[str]
        Redis connection string (used if client is None).
    key : Optional[str]
        Exact Redis storage key.
    name : Optional[str]
        Limiter name used to build Redis key (f"flowguard:limiter:{name}").
    on_redis_error : str
        Degradation strategy on Redis failure: 'fallback_local' (default), 'fail_open', or 'fail_closed'.
    local_capacity_ratio : Optional[float]
        Capacity/rate multiplier for in-memory fallback TokenBucketLimiter (defaults to 1.0).
    recovery_interval : float
        Seconds to wait after a Redis failure before attempting reconnection (defaults to 5.0).
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        initial_tokens: Optional[float] = None,
        client: Optional[Any] = None,
        redis_url: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        on_redis_error: str = "fallback_local",
        local_capacity_ratio: Optional[float] = None,
        recovery_interval: float = 5.0,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"Rate must be positive, got {rate}")
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        if on_redis_error not in ("fallback_local", "fail_open", "fail_closed"):
            raise ValueError(
                f"Invalid on_redis_error: {on_redis_error!r}. "
                f"Must be one of 'fallback_local', 'fail_open', or 'fail_closed'"
            )
        if local_capacity_ratio is not None and local_capacity_ratio <= 0:
            raise ValueError(f"local_capacity_ratio must be positive, got {local_capacity_ratio}")
        if recovery_interval < 0:
            raise ValueError(f"recovery_interval must be non-negative, got {recovery_interval}")

        self.rate = float(rate)
        self.capacity = float(capacity)
        self.initial_tokens = (
            float(initial_tokens) if initial_tokens is not None else float(capacity)
        )
        if self.initial_tokens > self.capacity:
            self.initial_tokens = self.capacity

        if key is not None:
            self.key = key
        elif name is not None:
            self.key = f"flowguard:limiter:{name}"
        else:
            self.key = "flowguard:limiter:default"

        self.on_redis_error = on_redis_error
        self.local_capacity_ratio = (
            float(local_capacity_ratio) if local_capacity_ratio is not None else 1.0
        )
        self.recovery_interval = float(recovery_interval)

        self._fallback_active: bool = False
        self._last_error_time: float = 0.0
        self._last_error: Optional[Exception] = None
        self._sync_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

        # Setup local in-memory fallback limiter scaled by local_capacity_ratio
        fallback_rate = self.rate * self.local_capacity_ratio
        fallback_cap = self.capacity * self.local_capacity_ratio
        fallback_init = min(self.initial_tokens, fallback_cap)
        self._local_limiter = TokenBucketLimiter(
            rate=fallback_rate,
            capacity=fallback_cap,
            initial_tokens=fallback_init,
        )

        # Dynamic import of redis.asyncio if client is not provided
        self._owns_client: bool = False
        if client is not None:
            self._client = client
        else:
            try:
                import redis.asyncio as aioredis

                self._client = aioredis.from_url(redis_url or "redis://localhost:6379/0")
                self._owns_client = True
            except ImportError as err:
                raise ImportError(
                    "redis package is required for RedisTokenBucketLimiter when 'client' is not provided. "
                    "Install it via 'pip install redis' or 'pip install flowguard-core[redis]'."
                ) from err

    @property
    def is_fallback_active(self) -> bool:
        """Indicate whether the limiter is currently degraded to fallback mode."""
        return self._fallback_active

    @property
    def local_limiter(self) -> TokenBucketLimiter:
        """Access the underlying in-memory TokenBucketLimiter used for local fallback."""
        return self._local_limiter

    @property
    def current_tokens(self) -> float:
        """
        Estimated available tokens.
        If degraded, returns local tokens; otherwise queries local limiter mirror.
        """
        return self._local_limiter.current_tokens

    def set_fallback(self, active: bool) -> None:
        """Manually force or reset fallback mode (primarily for testing and operations)."""
        self._fallback_active = active
        if active:
            self._last_error_time = time.monotonic()

    @staticmethod
    def _parse_eval_result(result: Any) -> Tuple[bool, float, float]:
        """Parse Lua script return tuple: [allowed, tokens, wait_time]."""
        if not isinstance(result, (list, tuple)) or len(result) < 3:
            raise ValueError(f"Unexpected Redis Lua script response format: {result!r}")
        allowed = bool(result[0])
        tokens_val = result[1].decode("utf-8") if isinstance(result[1], bytes) else str(result[1])
        wait_val = result[2].decode("utf-8") if isinstance(result[2], bytes) else str(result[2])
        return allowed, float(tokens_val), float(wait_val)

    def _handle_redis_error(self, exc: Exception) -> None:
        """Handle Redis connection/runtime error and activate degradation state."""
        self._fallback_active = True
        self._last_error_time = time.monotonic()
        self._last_error = exc
        logger.warning(
            "Redis rate limiter error for key '%s': %s. Degrading via on_redis_error='%s'.",
            self.key,
            exc,
            self.on_redis_error,
        )

    async def _execute_degraded_acquire(self, tokens: float, timeout: Optional[float]) -> bool:
        """Execute acquisition under degradation mode."""
        if self.on_redis_error == "fallback_local":
            return await self._local_limiter.acquire(tokens=tokens, timeout=timeout)
        elif self.on_redis_error == "fail_open":
            return True
        elif self.on_redis_error == "fail_closed":
            raise RateLimitExceededError(
                f"Redis rate limiter unavailable and on_redis_error='fail_closed': {self._last_error}"
            )
        else:
            raise ValueError(f"Unknown degradation mode: {self.on_redis_error}")

    def _execute_degraded_try_acquire(self, tokens: float) -> bool:
        """Execute non-blocking try_acquire under degradation mode."""
        if self.on_redis_error == "fallback_local":
            return self._local_limiter.try_acquire(tokens=tokens)
        elif self.on_redis_error == "fail_open":
            return True
        elif self.on_redis_error == "fail_closed":
            return False
        else:
            raise ValueError(f"Unknown degradation mode: {self.on_redis_error}")

    def _get_sync_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if self._sync_executor is None:
            self._sync_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="flowguard-redis-sync"
            )
        return self._sync_executor

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine synchronously, safe whether an event loop is running or not."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None or not loop.is_running():
            return asyncio.run(coro)

        return self._get_sync_executor().submit(lambda: asyncio.run(coro)).result()

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """
        Acquire specified tokens in distributed rate-limited queue order.

        Parameters
        ----------
        tokens : float
            Quantity of tokens to acquire.
        timeout : Optional[float]
            Maximum seconds to wait for sufficient tokens before raising RateLimitExceededError.

        Returns
        -------
        bool
            True if tokens successfully acquired.

        Raises
        ------
        ValueError
            If requested tokens <= 0 or exceed total bucket capacity.
        RateLimitExceededError
            If tokens cannot be acquired within timeout, or in fail_closed mode during outage.
        """
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            raise ValueError(
                f"Requested tokens ({tokens}) exceeds bucket capacity ({self.capacity})"
            )

        # Check if fallback is currently active and within recovery cooldown
        if self._fallback_active:
            if (time.monotonic() - self._last_error_time) < self.recovery_interval:
                return await self._execute_degraded_acquire(tokens, timeout)

        start_time = time.monotonic()

        while True:
            try:
                now = time.time()
                res = await self._client.eval(
                    LUA_ACQUIRE_SCRIPT,
                    1,
                    self.key,
                    str(tokens),
                    str(self.rate),
                    str(self.capacity),
                    str(now),
                    str(self.initial_tokens),
                )
                allowed, remaining_tokens, wait_time = self._parse_eval_result(res)

                # Successfully executed on Redis; clear any active fallback
                if self._fallback_active:
                    logger.info("Redis connectivity recovered for limiter key '%s'", self.key)
                    self._fallback_active = False

                if allowed:
                    return True

                # Bucket does not currently have enough tokens; evaluate timeout
                if timeout is not None:
                    elapsed = time.monotonic() - start_time
                    remaining = timeout - elapsed
                    if remaining <= 0 or wait_time > remaining:
                        raise RateLimitExceededError(
                            f"Failed to acquire {tokens} tokens within timeout of {timeout}s",
                            retry_after=max(0.001, wait_time),
                        )
                    sleep_dur = min(max(0.001, wait_time), remaining)
                else:
                    sleep_dur = max(0.001, wait_time)

                await asyncio.sleep(sleep_dur)

            except (RateLimitExceededError, ValueError, asyncio.CancelledError):
                raise
            except Exception as exc:
                self._handle_redis_error(exc)
                return await self._execute_degraded_acquire(tokens, timeout)

    async def async_try_acquire(self, tokens: float = 1.0) -> bool:
        """
        Asynchronous non-blocking token acquisition attempt.

        Returns True immediately if tokens were successfully consumed, False otherwise.
        """
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            return False

        if self._fallback_active:
            if (time.monotonic() - self._last_error_time) < self.recovery_interval:
                return self._execute_degraded_try_acquire(tokens)

        try:
            now = time.time()
            res = await self._client.eval(
                LUA_ACQUIRE_SCRIPT,
                1,
                self.key,
                str(tokens),
                str(self.rate),
                str(self.capacity),
                str(now),
                str(self.initial_tokens),
            )
            allowed, _, _ = self._parse_eval_result(res)
            if self._fallback_active:
                self._fallback_active = False
            return allowed
        except (ValueError, asyncio.CancelledError):
            raise
        except Exception as exc:
            self._handle_redis_error(exc)
            return self._execute_degraded_try_acquire(tokens)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """
        Synchronous non-blocking token acquisition check satisfying BaseRateLimiter.

        Returns True if tokens consumed, False otherwise. Degrades gracefully on Redis error.
        """
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            return False

        if self._fallback_active:
            if (time.monotonic() - self._last_error_time) < self.recovery_interval:
                return self._execute_degraded_try_acquire(tokens)

        try:
            return bool(self._run_sync(self.async_try_acquire(tokens)))
        except (ValueError, asyncio.CancelledError):
            raise
        except Exception as exc:
            self._handle_redis_error(exc)
            return self._execute_degraded_try_acquire(tokens)

    async def refund(self, tokens: float) -> None:
        """
        Return tokens to the bucket (e.g. for Prompt Caching discounts or over-estimated quota).

        Returns tokens to Redis via atomic Lua script. In fallback mode, returns tokens to the
        local in-memory bucket and immediately drains and wakes any local waiting tasks.
        """
        if tokens <= 0:
            return

        if self._fallback_active:
            await self._local_limiter.refund(tokens)
            return

        try:
            now = time.time()
            await self._client.eval(
                LUA_REFUND_SCRIPT,
                1,
                self.key,
                str(tokens),
                str(self.capacity),
                str(self.rate),
                str(now),
            )
            # Sync to local bucket as well
            self._local_limiter.refund_sync(tokens)
        except (ValueError, asyncio.CancelledError):
            raise
        except Exception as exc:
            self._handle_redis_error(exc)
            if self.on_redis_error == "fallback_local":
                await self._local_limiter.refund(tokens)

    def refund_sync(self, tokens: float) -> None:
        """Synchronously return tokens to bucket."""
        if tokens <= 0:
            return
        if self._fallback_active:
            self._local_limiter.refund_sync(tokens)
            return
        try:
            self._run_sync(self.refund(tokens))
        except Exception as exc:
            self._handle_redis_error(exc)
            if self.on_redis_error == "fallback_local":
                self._local_limiter.refund_sync(tokens)

    async def reset(self) -> None:
        """Reset limiter tokens in Redis and local fallback state."""
        await self._local_limiter.reset()

        if self._fallback_active:
            return

        try:
            now = time.time()
            await self._client.eval(
                LUA_RESET_SCRIPT,
                1,
                self.key,
                str(self.capacity),
                str(now),
            )
        except (ValueError, asyncio.CancelledError):
            raise
        except Exception as exc:
            self._handle_redis_error(exc)

    async def aclose(self) -> None:
        """Clean up background sync executors and internally created Redis client."""
        if self._sync_executor is not None:
            self._sync_executor.shutdown(wait=False)
            self._sync_executor = None

        if self._owns_client and self._client is not None:
            if hasattr(self._client, "aclose"):
                await self._client.aclose()
            elif hasattr(self._client, "close"):
                res = self._client.close()
                if asyncio.iscoroutine(res):
                    await res

    async def close(self) -> None:
        """Alias for aclose."""
        await self.aclose()
