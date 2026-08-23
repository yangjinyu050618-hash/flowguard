"""FlowGuard: Composable Resilience Pipeline (Retry -> Limiter -> Bulkhead -> Breaker -> Call)."""

import functools
import inspect
import time
from typing import Any, Callable, Coroutine, Optional, TypeVar, cast
from flowguard.core.limiter import BaseRateLimiter, TokenBucketLimiter
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.retry import RetryPolicy
from flowguard.core.bulkhead import Bulkhead
from flowguard.exceptions import CircuitBreakerOpenError, BulkheadFullError
from flowguard.metrics.collector import MetricsCollector

F = TypeVar("F", bound=Callable[..., Any])


class FlowGuard:
    """
    Unified Orchestrator combining Retry, Rate Limiting, Bulkhead Concurrency, and Circuit Breaking.

    Architecture Hierarchy:
    Retry (outer) -> RateLimiter -> Bulkhead -> CircuitBreaker -> Target Downstream
    """

    def __init__(
        self,
        name: str = "default",
        limiter: Optional[BaseRateLimiter] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry: Optional[RetryPolicy] = None,
        bulkhead: Optional[Bulkhead] = None,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self.name = name
        self.limiter = limiter
        self.circuit_breaker = circuit_breaker
        self.retry = retry
        self.bulkhead = bulkhead
        self.metrics = metrics or MetricsCollector(name=name)

    async def execute(self, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any) -> Any:
        tokens = kwargs.pop("__flowguard_tokens__", 1.0)

        async def _attempt() -> Any:
            # 1. Rate Limiting per physical attempt
            if self.limiter:
                try:
                    await self.limiter.acquire(tokens=tokens)
                except Exception as e:
                    self.metrics.record_rejected("rate_limit")
                    raise e

            # 2. Bulkhead concurrency isolation
            if self.bulkhead:
                try:
                    async with self.bulkhead:
                        return await _circuit_and_call()
                except BulkheadFullError as e:
                    self.metrics.record_rejected("bulkhead")
                    raise e
            else:
                return await _circuit_and_call()

        async def _circuit_and_call() -> Any:
            # 3. Circuit breaker check
            if self.circuit_breaker:
                try:
                    await self.circuit_breaker.before_call()
                except CircuitBreakerOpenError as e:
                    self.metrics.record_rejected("circuit_breaker")
                    raise e

            call_start = time.monotonic()
            try:
                res = await func(*args, **kwargs)
                if self.circuit_breaker:
                    await self.circuit_breaker.record_success()
                latency = time.monotonic() - call_start
                self.metrics.record_success(latency)
                return res
            except Exception as exc:
                if self.circuit_breaker:
                    await self.circuit_breaker.record_failure(exc)
                latency = time.monotonic() - call_start
                self.metrics.record_failure(latency, type(exc).__name__)
                raise exc

        # 4. Outer retry loop
        if self.retry:
            return await self.retry.execute(_attempt)
        return await _attempt()

    def __call__(self, func: F) -> F:
        """Decorator interface for protecting async functions."""
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"FlowGuard decorator currently supports async functions only, got {func}")

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.execute(func, *args, **kwargs)

        return cast(F, wrapper)


def guard(
    name: str = "default",
    rate_per_sec: Optional[float] = None,
    burst_capacity: Optional[float] = None,
    max_retries: int = 0,
    max_attempts: Optional[int] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    max_concurrent: Optional[int] = None,
) -> Callable[[F], F]:
    """
    Convenient all-in-one decorator factory.
    """
    limiter = None
    if rate_per_sec is not None:
        limiter = TokenBucketLimiter(rate=rate_per_sec, capacity=burst_capacity or rate_per_sec)

    circuit_breaker = None
    if failure_threshold > 0:
        circuit_breaker = CircuitBreaker(failure_threshold=failure_threshold, recovery_timeout=recovery_timeout)

    # Resolve attempts
    attempts = max_attempts if max_attempts is not None else (max_retries + 1 if max_retries > 0 else 1)
    retry_policy = None
    if attempts > 1:
        retry_policy = RetryPolicy(max_attempts=attempts)

    bulkhead = None
    if max_concurrent is not None and max_concurrent > 0:
        bulkhead = Bulkhead(max_concurrent=max_concurrent)

    pipeline = FlowGuard(
        name=name,
        limiter=limiter,
        circuit_breaker=circuit_breaker,
        retry=retry_policy,
        bulkhead=bulkhead,
    )
    return pipeline
