"""High-performance asynchronous Rate Limiting algorithms (Token Bucket & Sliding Window)."""

import abc
import asyncio
import time
from typing import Optional
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
    def reset(self) -> None:
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
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        now = time.monotonic()
        self._refill(now)
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
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
                if elapsed + wait_time > timeout:
                    raise RateLimitExceededError(
                        f"Failed to acquire {tokens} tokens within timeout of {timeout}s",
                        retry_after=wait_time,
                    )

            await asyncio.sleep(min(wait_time, 0.05))

    def reset(self) -> None:
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    @property
    def current_tokens(self) -> float:
        self._refill(time.monotonic())
        return self.tokens


class SlidingWindowLimiter(BaseRateLimiter):
    """
    Sliding window log rate limiter for strict time-interval quotas (e.g. max 100 requests per 60s).

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
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    def _prune(self, now: float) -> None:
        threshold = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= threshold:
            self._timestamps.pop(0)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        int_tokens = int(tokens)
        now = time.monotonic()
        self._prune(now)
        if len(self._timestamps) + int_tokens <= self.max_requests:
            for _ in range(int_tokens):
                self._timestamps.append(now)
            return True
        return False

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        int_tokens = int(tokens)
        start = time.monotonic()

        while True:
            async with self._lock:
                now = time.monotonic()
                self._prune(now)
                if len(self._timestamps) + int_tokens <= self.max_requests:
                    for _ in range(int_tokens):
                        self._timestamps.append(now)
                    return True

                oldest = self._timestamps[0]
                wait_time = max(0.001, (oldest + self.window_seconds) - now)

            if timeout is not None:
                elapsed = time.monotonic() - start
                if elapsed + wait_time > timeout:
                    raise RateLimitExceededError(
                        f"Sliding window rate limit exceeded ({self.max_requests} reqs/{self.window_seconds}s)",
                        retry_after=wait_time,
                    )

            await asyncio.sleep(min(wait_time, 0.05))

    def reset(self) -> None:
        self._timestamps.clear()
