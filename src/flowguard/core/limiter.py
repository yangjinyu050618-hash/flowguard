"""High-performance asynchronous Rate Limiting algorithms (Token Bucket & Sliding Window)."""

import abc
import asyncio
import collections
import time
from typing import Deque, Optional
from flowguard.exceptions import RateLimitExceededError


class BaseRateLimiter(abc.ABC):
    """Abstract base class for asynchronous rate limiters."""

    @abc.abstractmethod
    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Acquire specified tokens. Blocks asynchronously until available or timeout."""
        pass

    @abc.abstractmethod
    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking token acquisition check."""
        pass

    @abc.abstractmethod
    async def reset(self) -> None:
        """Reset limiter internal state."""
        pass


class TokenBucketLimiter(BaseRateLimiter):
    """
    Asynchronous Token Bucket rate limiter with burst capability.

    Parameters
    ----------
    rate : float
        Refill rate (tokens per second).
    capacity : float
        Maximum token capacity (burst capacity).
    initial_tokens : Optional[float]
        Starting tokens in bucket (defaults to capacity).
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        initial_tokens: Optional[float] = None,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"Rate must be positive, got {rate}")
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")

        self.rate = float(rate)
        self.capacity = float(capacity)
        self.tokens = float(initial_tokens) if initial_tokens is not None else float(capacity)
        if self.tokens > self.capacity:
            self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            return False

        now = time.monotonic()
        self._refill(now)
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            raise ValueError(
                f"Requested tokens ({tokens}) exceeds bucket capacity ({self.capacity})"
            )

        start_time = time.monotonic()
        while True:
            async with self._lock:
                now = time.monotonic()
                self._refill(now)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

                needed = tokens - self.tokens
                wait_time = needed / self.rate

            if timeout is not None:
                elapsed = time.monotonic() - start_time
                remaining = timeout - elapsed
                if wait_time > remaining:
                    await asyncio.sleep(max(0.0, remaining))
                    raise RateLimitExceededError(
                        f"Failed to acquire {tokens} tokens within timeout of {timeout}s",
                        retry_after=wait_time,
                    )
                sleep_duration = min(wait_time, remaining)
            else:
                sleep_duration = wait_time

            await asyncio.sleep(sleep_duration)

    async def reset(self) -> None:
        async with self._lock:
            self.tokens = self.capacity
            self.last_refill = time.monotonic()

    @property
    def current_tokens(self) -> float:
        """Read-only view of estimated available tokens without modifying state."""
        now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill)
        return min(self.capacity, self.tokens + elapsed * self.rate)


class SlidingWindowLimiter(BaseRateLimiter):
    """
    Sliding window log rate limiter for strict time-interval quotas.

    Parameters
    ----------
    max_requests : int
        Maximum number of requests permitted in the window.
    window_seconds : float
        Window duration in seconds.
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        if max_requests <= 0 or window_seconds <= 0:
            raise ValueError("max_requests and window_seconds must be positive numbers.")
        self.max_requests = max_requests
        self.window_seconds = float(window_seconds)
        self._timestamps: Deque[float] = collections.deque()
        self._lock = asyncio.Lock()

    def _prune(self, now: float) -> None:
        threshold = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= threshold:
            self._timestamps.popleft()

    def try_acquire(self, tokens: float = 1.0) -> bool:
        int_tokens = int(tokens)
        if int_tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if int_tokens > self.max_requests:
            return False

        now = time.monotonic()
        self._prune(now)
        if len(self._timestamps) + int_tokens <= self.max_requests:
            for _ in range(int_tokens):
                self._timestamps.append(now)
            return True
        return False

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        int_tokens = int(tokens)
        if int_tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if int_tokens > self.max_requests:
            raise ValueError(
                f"Requested tokens ({int_tokens}) exceeds maximum window capacity ({self.max_requests})"
            )

        start = time.monotonic()
        while True:
            async with self._lock:
                now = time.monotonic()
                self._prune(now)
                if len(self._timestamps) + int_tokens <= self.max_requests:
                    for _ in range(int_tokens):
                        self._timestamps.append(now)
                    return True

                if self._timestamps:
                    oldest = self._timestamps[0]
                    wait_time = max(0.001, (oldest + self.window_seconds) - now)
                else:
                    wait_time = 0.001

            if timeout is not None:
                elapsed = time.monotonic() - start
                remaining = timeout - elapsed
                if wait_time > remaining:
                    await asyncio.sleep(max(0.0, remaining))
                    raise RateLimitExceededError(
                        f"Sliding window rate limit exceeded ({self.max_requests} reqs/{self.window_seconds}s)",
                        retry_after=wait_time,
                    )
                sleep_duration = min(wait_time, remaining)
            else:
                sleep_duration = wait_time

            await asyncio.sleep(sleep_duration)

    async def reset(self) -> None:
        async with self._lock:
            self._timestamps.clear()
