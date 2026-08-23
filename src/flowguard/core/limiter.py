"""High-performance asynchronous Rate Limiting algorithms (Token Bucket & Sliding Window) with strict FIFO queuing."""

import abc
import asyncio
import collections
import time
from typing import Deque, Optional, Tuple
from flowguard.exceptions import RateLimitExceededError


class BaseRateLimiter(abc.ABC):
    """Abstract base class for asynchronous rate limiters."""

    @abc.abstractmethod
    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Acquire specified tokens in strict FIFO queue order."""
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
    Asynchronous Token Bucket rate limiter with strict FIFO fairness.

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
        self._waiters: Deque[Tuple[float, asyncio.Future[None]]] = collections.deque()
        self._lock = asyncio.Lock()

    def _refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

    def _drain(self, now: float) -> Optional[float]:
        """Drain eligible waiters in FIFO order. Returns delay in seconds for next waiter, or None."""
        self._refill(now)
        while self._waiters:
            needed, fut = self._waiters[0]
            if fut.cancelled():
                self._waiters.popleft()
                continue
            if self.tokens >= needed:
                self.tokens -= needed
                self._waiters.popleft()
                if not fut.done():
                    fut.set_result(None)
            else:
                deficit = needed - self.tokens
                return deficit / self.rate
        return None

    def try_acquire(self, tokens: float = 1.0) -> bool:
        if tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if tokens > self.capacity:
            return False

        now = time.monotonic()
        self._refill(now)
        if not self._waiters and self.tokens >= tokens:
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

        loop = asyncio.get_running_loop()
        now = time.monotonic()

        async with self._lock:
            self._drain(now)
            if not self._waiters and self.tokens >= tokens:
                self.tokens -= tokens
                return True

            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append((tokens, fut))

        start_time = time.monotonic()
        while not fut.done():
            async with self._lock:
                if fut.done():
                    break
                now = time.monotonic()
                next_wait = self._drain(now)
                if fut.done():
                    break
                wait_time = next_wait if next_wait is not None else (tokens / self.rate)

            if timeout is not None:
                elapsed = time.monotonic() - start_time
                remaining = timeout - elapsed
                if wait_time > remaining:
                    async with self._lock:
                        if not fut.done():
                            fut.cancel()
                            self._drain(time.monotonic())
                    raise RateLimitExceededError(
                        f"Failed to acquire {tokens} tokens within timeout of {timeout}s",
                        retry_after=wait_time,
                    )
                sleep_dur = min(wait_time, remaining)
            else:
                sleep_dur = wait_time

            try:
                await asyncio.wait_for(asyncio.shield(fut), timeout=max(0.001, sleep_dur))
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                async with self._lock:
                    if not fut.done():
                        fut.cancel()
                        self._drain(time.monotonic())
                raise

        return True

    async def reset(self) -> None:
        async with self._lock:
            self.tokens = self.capacity
            self.last_refill = time.monotonic()
            while self._waiters:
                _, fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(None)

    @property
    def current_tokens(self) -> float:
        """Read-only view of estimated available tokens without modifying state."""
        now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill)
        return min(self.capacity, self.tokens + elapsed * self.rate)


class SlidingWindowLimiter(BaseRateLimiter):
    """
    Sliding window log rate limiter with strict FIFO fairness.

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
        self._waiters: Deque[Tuple[int, asyncio.Future[None]]] = collections.deque()
        self._lock = asyncio.Lock()

    def _prune(self, now: float) -> None:
        threshold = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= threshold:
            self._timestamps.popleft()

    def _drain(self, now: float) -> Optional[float]:
        self._prune(now)
        while self._waiters:
            needed, fut = self._waiters[0]
            if fut.cancelled():
                self._waiters.popleft()
                continue
            if len(self._timestamps) + needed <= self.max_requests:
                for _ in range(needed):
                    self._timestamps.append(now)
                self._waiters.popleft()
                if not fut.done():
                    fut.set_result(None)
            else:
                if self._timestamps:
                    oldest = self._timestamps[0]
                    return max(0.001, (oldest + self.window_seconds) - now)
                return 0.001
        return None

    def try_acquire(self, tokens: float = 1.0) -> bool:
        int_tokens = int(tokens)
        if int_tokens <= 0:
            raise ValueError(f"Requested tokens must be positive, got {tokens}")
        if int_tokens > self.max_requests:
            return False

        now = time.monotonic()
        self._prune(now)
        if not self._waiters and len(self._timestamps) + int_tokens <= self.max_requests:
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

        loop = asyncio.get_running_loop()
        now = time.monotonic()

        async with self._lock:
            self._drain(now)
            if not self._waiters and len(self._timestamps) + int_tokens <= self.max_requests:
                for _ in range(int_tokens):
                    self._timestamps.append(now)
                return True

            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append((int_tokens, fut))

        start = time.monotonic()
        while not fut.done():
            async with self._lock:
                if fut.done():
                    break
                now = time.monotonic()
                next_wait = self._drain(now)
                if fut.done():
                    break
                wait_time = next_wait if next_wait is not None else 0.001

            if timeout is not None:
                elapsed = time.monotonic() - start
                remaining = timeout - elapsed
                if wait_time > remaining:
                    async with self._lock:
                        if not fut.done():
                            fut.cancel()
                            self._drain(time.monotonic())
                    raise RateLimitExceededError(
                        f"Sliding window rate limit exceeded ({self.max_requests} reqs/{self.window_seconds}s)",
                        retry_after=wait_time,
                    )
                sleep_dur = min(wait_time, remaining)
            else:
                sleep_dur = wait_time

            try:
                await asyncio.wait_for(asyncio.shield(fut), timeout=max(0.001, sleep_dur))
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                async with self._lock:
                    if not fut.done():
                        fut.cancel()
                        self._drain(time.monotonic())
                raise

        return True

    async def reset(self) -> None:
        async with self._lock:
            self._timestamps.clear()
            while self._waiters:
                _, fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(None)
